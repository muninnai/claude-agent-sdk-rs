//! Comprehensive unit tests for ClaudeClient using the mock framework
//!
//! These tests validate the ClaudeClient behavior without requiring the Claude Code CLI.
//! They use the mock framework to simulate CLI communication.

use claude_agent_sdk_rs::testing::{
    AssistantMessageBuilder, MockClient, MockTransport, ResultMessageBuilder, ScenarioBuilder,
    SystemMessageBuilder, Transport, timing_profiles,
};
use claude_agent_sdk_rs::{
    ClaudeAgentOptions, ClaudeClient, ClaudeError, InterruptError, InterruptRequestAccepted,
    Message, PermissionMode,
};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Connection Lifecycle Tests
// =============================================================================

#[tokio::test]
async fn test_client_connect_and_disconnect() {
    let scenario = ScenarioBuilder::new("connect_disconnect")
        .on_connect(SystemMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);

    // Should not be connected initially
    client.connect_with_transport().await.unwrap();

    // Should be able to disconnect
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_double_connect_is_idempotent() {
    let scenario = ScenarioBuilder::new("double_connect")
        .on_connect(SystemMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);

    client.connect_with_transport().await.unwrap();
    // Second connect should be a no-op (not error)
    client.connect_with_transport().await.unwrap();

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_double_disconnect_is_idempotent() {
    let scenario = ScenarioBuilder::new("double_disconnect")
        .on_connect(SystemMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.disconnect().await.unwrap();
    // Second disconnect should be a no-op (not error)
    client.disconnect().await.unwrap();
}

// =============================================================================
// Query Lifecycle Tests
// =============================================================================

#[tokio::test]
async fn test_client_query_sends_correct_format() {
    let scenario = ScenarioBuilder::new("query_format")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("Hello!").build())
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("What is 2 + 2?").await.unwrap();

    // Verify the query was written in correct format
    let written = client.transport().written_messages_async().await;
    assert!(
        !written.is_empty(),
        "Should have written at least one message"
    );

    // Check the JSON format
    let last_write = &written[written.len() - 1];
    assert!(last_write.parsed.is_some(), "Should be valid JSON");

    let json = last_write.parsed.as_ref().unwrap();
    assert_eq!(json["type"], "user", "Should be a user message");
    assert!(
        json["message"]["content"].as_str().is_some(),
        "Should have content"
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_query_with_session_id() {
    let scenario = ScenarioBuilder::new("query_session")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("Response").build())
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client
        .query_with_session("Hello", "my-session-123")
        .await
        .unwrap();

    // Verify session_id was included
    let written = client.transport().written_messages_async().await;
    let last_write = &written[written.len() - 1];
    let json = last_write.parsed.as_ref().unwrap();

    assert_eq!(
        json["session_id"], "my-session-123",
        "Should include session_id"
    );

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_query_without_connect_fails() {
    let scenario = ScenarioBuilder::new("no_connect")
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);

    // Query without connecting should fail
    let result = client.query("Hello").await;
    assert!(result.is_err(), "Query without connect should fail");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not connected") || err_msg.contains("connect"),
        "Error should mention connection: {}",
        err_msg
    );
}

// =============================================================================
// Response Stream Tests
// =============================================================================

#[tokio::test]
async fn test_client_receive_response_until_result() {
    let scenario = ScenarioBuilder::new("response_stream")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("First part").build())
        .respond(AssistantMessageBuilder::new().text("Second part").build())
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("Hello").await.unwrap();

    let messages: Vec<_> = client.receive_response().collect().await;

    // Should have assistant messages and result
    let assistant_count = messages
        .iter()
        .filter(|r| {
            r.as_ref()
                .map(|m| matches!(m, Message::Assistant(_)))
                .unwrap_or(false)
        })
        .count();
    let result_count = messages
        .iter()
        .filter(|r| {
            r.as_ref()
                .map(|m| matches!(m, Message::Result(_)))
                .unwrap_or(false)
        })
        .count();

    assert!(
        assistant_count >= 2,
        "Should have at least 2 assistant messages"
    );
    assert_eq!(result_count, 1, "Should have exactly 1 result message");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_receive_messages_continuous() {
    let transport = MockTransport::builder()
        .message(serde_json::json!({"type": "system", "subtype": "init"}))
        .message(serde_json::json!({"type": "assistant", "message": {"content": []}}))
        .build();

    transport.connect().await.unwrap();

    let mut stream = transport.read_messages();

    // Should receive system message
    let msg1 = stream.next().await.unwrap().unwrap();
    assert_eq!(msg1["type"], "system");

    // Should receive first assistant message
    let msg2 = stream.next().await.unwrap().unwrap();
    assert_eq!(msg2["type"], "assistant");

    // Inject more messages dynamically
    transport.inject(serde_json::json!({"type": "assistant", "message": {"content": [{"type": "text", "text": "Hello"}]}}));

    // Should receive injected message
    let msg3 = tokio::time::timeout(Duration::from_millis(100), stream.next())
        .await
        .expect("Should receive injected message")
        .unwrap()
        .unwrap();
    assert_eq!(msg3["type"], "assistant");

    transport.close().await.unwrap();
}

// =============================================================================
// Multi-turn Conversation Tests
// =============================================================================

#[tokio::test]
async fn test_client_multi_turn_conversation() {
    let scenario = ScenarioBuilder::new("multi_turn")
        .on_connect(SystemMessageBuilder::default().build())
        // First exchange
        .exchange()
        .respond(
            AssistantMessageBuilder::new()
                .text("Hello! How can I help?")
                .build(),
        )
        .then_result(ResultMessageBuilder::default().build())
        // Second exchange
        .exchange()
        .respond(
            AssistantMessageBuilder::new()
                .text("I can help with that!")
                .build(),
        )
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    // First turn
    client.query("Hi!").await.unwrap();
    let messages1: Vec<_> = client.receive_response().collect().await;
    assert!(!messages1.is_empty());

    // Second turn
    client.query("Can you help?").await.unwrap();
    let messages2: Vec<_> = client.receive_response().collect().await;
    assert!(!messages2.is_empty());

    client.disconnect().await.unwrap();
}

// =============================================================================
// Tool Use Flow Tests
// =============================================================================

#[tokio::test]
async fn test_client_tool_use_response() {
    let scenario = ScenarioBuilder::new("tool_use")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(
            AssistantMessageBuilder::new()
                .text("Let me read that file.")
                .tool_use("Read", serde_json::json!({"file_path": "/test.txt"}))
                .build(),
        )
        .respond(
            AssistantMessageBuilder::new()
                .text("The file contains: test content")
                .build(),
        )
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("Read /test.txt").await.unwrap();

    let messages: Vec<_> = client
        .receive_response()
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    // Should have tool use in one of the messages
    let has_tool_use = messages.iter().any(|m| {
        if let Message::Assistant(a) = m {
            a.message
                .content
                .iter()
                .any(|c| matches!(c, claude_agent_sdk_rs::ContentBlock::ToolUse(_)))
        } else {
            false
        }
    });

    assert!(has_tool_use, "Should have a tool use block");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_tool_use_with_specific_id() {
    let scenario = ScenarioBuilder::new("tool_use_id")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(
            AssistantMessageBuilder::new()
                .tool_use_with_id(
                    "my_tool_123",
                    "Read",
                    serde_json::json!({"file_path": "/test.txt"}),
                )
                .build(),
        )
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("Read file").await.unwrap();

    let messages: Vec<_> = client
        .receive_response()
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    // Find the tool use and check its ID
    for msg in messages {
        if let Message::Assistant(a) = msg {
            for block in &a.message.content {
                if let claude_agent_sdk_rs::ContentBlock::ToolUse(tool) = block {
                    assert_eq!(tool.id, "my_tool_123", "Tool ID should match");
                }
            }
        }
    }

    client.disconnect().await.unwrap();
}

// =============================================================================
// Configuration Tests
// =============================================================================

#[tokio::test]
async fn test_client_with_custom_options() {
    let scenario = ScenarioBuilder::new("custom_options")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("OK").build())
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let options = ClaudeAgentOptions::builder()
        .max_turns(5)
        .permission_mode(PermissionMode::BypassPermissions)
        .model("claude-sonnet-4")
        .build();

    let mut client = MockClient::from_scenario_with_options(scenario, options.clone());
    client.connect_with_transport().await.unwrap();

    assert_eq!(client.options().max_turns, Some(5));
    assert_eq!(
        client.options().permission_mode,
        Some(PermissionMode::BypassPermissions)
    );

    client.disconnect().await.unwrap();
}

// =============================================================================
// Assertion Helper Tests
// =============================================================================

#[tokio::test]
async fn test_mock_client_assert_wrote() {
    let scenario = ScenarioBuilder::new("assert_wrote")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("OK").build())
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("test message content").await.unwrap();

    // Should pass - the message was written
    client.assert_wrote("test message content");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_mock_client_assert_wrote_json() {
    let scenario = ScenarioBuilder::new("assert_wrote_json")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("OK").build())
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("Hello").await.unwrap();

    // Should pass - check JSON structure
    client.assert_wrote_json(|json| json["type"] == "user");

    client.disconnect().await.unwrap();
}

// =============================================================================
// Message Injection Tests
// =============================================================================

#[tokio::test]
async fn test_mock_client_inject_message() {
    let scenario = ScenarioBuilder::new("inject")
        .on_connect(SystemMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    // Inject a custom message
    client.inject_message(
        AssistantMessageBuilder::new()
            .text("Injected message")
            .build(),
    );

    let mut stream = client.receive_messages();
    let msg = tokio::time::timeout(Duration::from_millis(100), stream.next())
        .await
        .expect("Should receive injected message")
        .unwrap()
        .unwrap();

    if let Message::Assistant(a) = msg {
        assert!(!a.message.content.is_empty());
    } else {
        panic!("Expected assistant message");
    }
}

#[tokio::test]
async fn test_mock_client_inject_error() {
    let scenario = ScenarioBuilder::new("inject_error")
        .on_connect(SystemMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    // Inject an error
    client.inject_error("Something went wrong");

    // The error should be receivable
    let mut stream = client.receive_messages();
    let msg = tokio::time::timeout(Duration::from_millis(100), stream.next())
        .await
        .expect("Should receive error message");

    // Either an error in parsing or a proper error message
    assert!(msg.is_some());
}

// =============================================================================
// Extended Thinking Tests
// =============================================================================

#[tokio::test]
async fn test_client_thinking_block() {
    let scenario = ScenarioBuilder::new("thinking")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(
            AssistantMessageBuilder::new()
                .thinking("Let me analyze this problem...")
                .text("Here's my answer.")
                .build(),
        )
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("Complex question").await.unwrap();

    let messages: Vec<_> = client
        .receive_response()
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    // Find thinking block
    let has_thinking = messages.iter().any(|m| {
        if let Message::Assistant(a) = m {
            a.message
                .content
                .iter()
                .any(|c| matches!(c, claude_agent_sdk_rs::ContentBlock::Thinking(_)))
        } else {
            false
        }
    });

    assert!(has_thinking, "Should have a thinking block");

    client.disconnect().await.unwrap();
}

// =============================================================================
// Result Message Tests
// =============================================================================

#[tokio::test]
async fn test_result_message_with_cost() {
    let scenario = ScenarioBuilder::new("result_cost")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("OK").build())
        .then_result(
            ResultMessageBuilder::default()
                .cost_usd(0.05)
                .duration_ms(1500)
                .turns(3)
                .build(),
        )
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("Test").await.unwrap();

    let messages: Vec<_> = client
        .receive_response()
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    // Find result message
    let result = messages.iter().find_map(|m| {
        if let Message::Result(r) = m {
            Some(r)
        } else {
            None
        }
    });

    assert!(result.is_some(), "Should have result message");
    let result = result.unwrap();

    assert!(result.total_cost_usd.is_some());
    assert!(result.total_cost_usd.unwrap() > 0.0);
    assert!(result.duration_ms > 0);
    assert!(result.num_turns > 0);
}

#[tokio::test]
async fn test_result_message_error() {
    let scenario = ScenarioBuilder::new("result_error")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .respond(AssistantMessageBuilder::new().text("Error").build())
        .then_result(ResultMessageBuilder::default().error().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    client.query("Test").await.unwrap();

    let messages: Vec<_> = client
        .receive_response()
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    let result = messages.iter().find_map(|m| {
        if let Message::Result(r) = m {
            Some(r)
        } else {
            None
        }
    });

    assert!(result.is_some());
    assert!(result.unwrap().is_error);
}

// =============================================================================
// System Message Tests
// =============================================================================

#[tokio::test]
async fn test_system_message_with_tools() {
    let scenario = ScenarioBuilder::new("system_tools")
        .on_connect(
            SystemMessageBuilder::default()
                .session_id("test-session")
                .tools(vec!["Read", "Write", "Bash", "Edit"])
                .build(),
        )
        .timing(timing_profiles::instant())
        .build();

    let transport = MockTransport::from_scenario(scenario);
    transport.connect().await.unwrap();

    let mut stream = transport.read_messages();
    let msg = stream.next().await.unwrap().unwrap();

    assert_eq!(msg["type"], "system");
    assert_eq!(msg["session_id"], "test-session");
    assert!(msg["tools"].is_array());
    assert_eq!(msg["tools"].as_array().unwrap().len(), 4);
}

// =============================================================================
// Trigger Pattern Tests
// =============================================================================

#[tokio::test]
async fn test_scenario_with_trigger_pattern() {
    let scenario = ScenarioBuilder::new("trigger")
        .on_connect(SystemMessageBuilder::default().build())
        .exchange()
        .when_write_contains("magic_word")
        .respond(
            AssistantMessageBuilder::new()
                .text("You found the magic word!")
                .build(),
        )
        .then_result(ResultMessageBuilder::default().build())
        .timing(timing_profiles::instant())
        .build();

    let mut client = MockClient::from_scenario(scenario);
    client.connect_with_transport().await.unwrap();

    // This query contains the magic word
    client.query("Say magic_word please").await.unwrap();

    let messages: Vec<_> = tokio::time::timeout(
        Duration::from_secs(2),
        client.receive_response().collect::<Vec<_>>(),
    )
    .await
    .expect("Should receive response");

    assert!(!messages.is_empty(), "Should have received messages");

    client.disconnect().await.unwrap();
}

// =============================================================================
// set_permission_mode Response Parsing Tests
// =============================================================================

/// Watches for a `set_permission_mode` control request in transport writes,
/// extracts the request_id, and injects a control_response with the given body
/// fields merged in.
fn spawn_permission_mode_response_watcher(
    transport: Arc<MockTransport>,
    response_fields: serde_json::Value,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seen = 0;
        loop {
            let written = transport.written_messages_async().await;
            for i in seen..written.len() {
                if let Some(ref parsed) = written[i].parsed {
                    if parsed.get("type").and_then(|v| v.as_str()) == Some("control_request")
                        && parsed.pointer("/request/subtype").and_then(|v| v.as_str())
                            == Some("set_permission_mode")
                    {
                        let request_id = parsed["request_id"].as_str().unwrap();
                        let mut response = serde_json::json!({
                            "type": "control_response",
                            "response": {
                                "subtype": "set_permission_mode",
                                "request_id": request_id,
                            }
                        });
                        if let Some(resp_obj) = response["response"].as_object_mut() {
                            if let Some(body_obj) = response_fields.as_object() {
                                for (k, v) in body_obj {
                                    resp_obj.insert(k.clone(), v.clone());
                                }
                            }
                        }
                        transport.inject(response);
                        return;
                    }
                }
            }
            seen = written.len();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

#[tokio::test]
async fn test_set_permission_mode_success_returns_confirmed_mode() {
    let transport = Arc::new(MockTransport::builder().build());

    let _watcher = spawn_permission_mode_response_watcher(
        Arc::clone(&transport),
        serde_json::json!({"response": {"mode": "bypassPermissions"}}),
    );

    let mut client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.set_permission_mode(PermissionMode::BypassPermissions),
    )
    .await
    .expect("should not timeout");

    assert_eq!(result.unwrap(), PermissionMode::BypassPermissions);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_set_permission_mode_error_response_returns_error() {
    let transport = Arc::new(MockTransport::builder().build());

    let _watcher = spawn_permission_mode_response_watcher(
        Arc::clone(&transport),
        serde_json::json!({
            "error": "Cannot set permission mode to bypassPermissions because the session was not launched with --dangerously-skip-permissions"
        }),
    );

    let mut client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.set_permission_mode(PermissionMode::BypassPermissions),
    )
    .await
    .expect("should not timeout");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("dangerously-skip-permissions"),
        "Error should contain rejection text, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_set_permission_mode_missing_response_mode_returns_error() {
    let transport = Arc::new(MockTransport::builder().build());

    let _watcher = spawn_permission_mode_response_watcher(
        Arc::clone(&transport),
        serde_json::json!({"response": {}}),
    );

    let mut client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.set_permission_mode(PermissionMode::Default),
    )
    .await
    .expect("should not timeout");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("missing response.mode"),
        "Error should mention missing mode, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_set_permission_mode_invalid_mode_value_returns_error() {
    let transport = Arc::new(MockTransport::builder().build());

    let _watcher = spawn_permission_mode_response_watcher(
        Arc::clone(&transport),
        serde_json::json!({"response": {"mode": "notARealMode"}}),
    );

    let mut client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.set_permission_mode(PermissionMode::Default),
    )
    .await
    .expect("should not timeout");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("invalid mode"),
        "Error should mention invalid mode, got: {err_msg}"
    );
}

// =============================================================================
// interrupt Outcome Tests
// =============================================================================

/// Watches for an `interrupt` control request in transport writes, extracts the
/// request_id, and injects a control_response built from the given envelope
/// fields.
///
/// `envelope_fields` is merged into the `response` object *beside* the
/// request_id, so a test controls the discriminator and the payload
/// independently — including omitting `subtype` or supplying one the fold
/// cannot interpret.
fn spawn_interrupt_response_watcher(
    transport: Arc<MockTransport>,
    envelope_fields: serde_json::Value,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seen = 0;
        loop {
            let written = transport.written_messages_async().await;
            for i in seen..written.len() {
                if let Some(ref parsed) = written[i].parsed {
                    if parsed.get("type").and_then(|v| v.as_str()) == Some("control_request")
                        && parsed.pointer("/request/subtype").and_then(|v| v.as_str())
                            == Some("interrupt")
                    {
                        let request_id = parsed["request_id"].as_str().unwrap();
                        let mut response = serde_json::json!({
                            "type": "control_response",
                            "response": {
                                "request_id": request_id,
                            }
                        });
                        if let Some(resp_obj) = response["response"].as_object_mut() {
                            if let Some(body_obj) = envelope_fields.as_object() {
                                for (k, v) in body_obj {
                                    resp_obj.insert(k.clone(), v.clone());
                                }
                            }
                        }
                        transport.inject(response);
                        return;
                    }
                }
            }
            seen = written.len();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

/// Drive one interrupt against a mock transport that answers with
/// `envelope_fields`, and hand back the outcome.
async fn interrupt_against_envelope(
    envelope_fields: serde_json::Value,
) -> std::result::Result<InterruptRequestAccepted, InterruptError> {
    let transport = Arc::new(MockTransport::builder().build());

    let _watcher = spawn_interrupt_response_watcher(Arc::clone(&transport), envelope_fields);

    let mut client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), client.interrupt())
        .await
        .expect("should not timeout");

    client.disconnect().await.unwrap();
    result
}

#[tokio::test]
async fn test_interrupt_success_with_receipt_preserves_still_queued() {
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "success",
        "response": {"still_queued": ["uuid-a", "uuid-b"]},
    }))
    .await;

    assert_eq!(
        result.unwrap(),
        InterruptRequestAccepted {
            still_queued: Some(vec!["uuid-a".to_string(), "uuid-b".to_string()]),
        }
    );
}

#[tokio::test]
async fn test_interrupt_success_with_empty_receipt_is_some_empty() {
    // `Some(vec![])` and `None` are different claims: an empty list is a
    // receipt reporting no covered queued survivor, while `None` means no
    // usable receipt was recovered at all.
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "success",
        "response": {"still_queued": []},
    }))
    .await;

    assert_eq!(
        result.unwrap(),
        InterruptRequestAccepted {
            still_queued: Some(vec![]),
        }
    );
}

#[tokio::test]
async fn test_interrupt_success_without_payload_is_accepted() {
    // Acceptance is strict on the envelope and permissive on its payload:
    // older CLIs answer with no `response` object at all.
    let result = interrupt_against_envelope(serde_json::json!({"subtype": "success"})).await;

    assert_eq!(
        result.unwrap(),
        InterruptRequestAccepted { still_queued: None }
    );
}

#[tokio::test]
async fn test_interrupt_success_with_null_payload_is_accepted() {
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "success",
        "response": serde_json::Value::Null,
    }))
    .await;

    assert_eq!(
        result.unwrap(),
        InterruptRequestAccepted { still_queued: None }
    );
}

#[tokio::test]
async fn test_interrupt_success_with_unknown_payload_fields_is_accepted() {
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "success",
        "response": {"some_future_field": 7},
    }))
    .await;

    assert_eq!(
        result.unwrap(),
        InterruptRequestAccepted { still_queued: None }
    );
}

#[tokio::test]
async fn test_interrupt_mixed_receipt_array_yields_no_receipt() {
    // All-or-nothing parsing. Dropping the non-string element would report a
    // shorter survivor list as though it were the whole one.
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "success",
        "response": {"still_queued": ["uuid-a", 7, "uuid-b"]},
    }))
    .await;

    assert_eq!(
        result.unwrap(),
        InterruptRequestAccepted { still_queued: None },
        "a mixed array must yield no receipt, never a filtered partial one"
    );
}

#[tokio::test]
async fn test_interrupt_receipt_that_is_not_an_array_yields_no_receipt() {
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "success",
        "response": {"still_queued": "uuid-a"},
    }))
    .await;

    assert_eq!(
        result.unwrap(),
        InterruptRequestAccepted { still_queued: None }
    );
}

#[tokio::test]
async fn test_interrupt_error_envelope_reports_the_error_response() {
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "error",
        "error": "interrupt rejected",
    }))
    .await;

    match result.unwrap_err() {
        InterruptError::ErrorResponse { message } => assert_eq!(message, "interrupt rejected"),
        other => panic!("expected ErrorResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn test_interrupt_error_envelope_without_message_is_unusable() {
    // An `error` discriminator whose `error` field is absent cannot be reported
    // as an error response, because there is no message the wire supplied.
    let result = interrupt_against_envelope(serde_json::json!({"subtype": "error"})).await;

    match result.unwrap_err() {
        InterruptError::UnexpectedResponse { subtype } => assert_eq!(subtype, "error"),
        other => panic!("expected UnexpectedResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn test_interrupt_error_envelope_with_non_string_message_is_unusable() {
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "error",
        "error": {"code": 7},
    }))
    .await;

    match result.unwrap_err() {
        InterruptError::UnexpectedResponse { subtype } => assert_eq!(subtype, "error"),
        other => panic!("expected UnexpectedResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn test_interrupt_unknown_subtype_is_unusable() {
    // The fold is total: a correlated response carrying neither `success` nor
    // `error` is reported as unusable rather than guessed in either direction.
    let result = interrupt_against_envelope(serde_json::json!({
        "subtype": "set_permission_mode",
        "response": {"mode": "default"},
    }))
    .await;

    match result.unwrap_err() {
        InterruptError::UnexpectedResponse { subtype } => {
            assert_eq!(subtype, "set_permission_mode")
        }
        other => panic!("expected UnexpectedResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn test_interrupt_before_connecting_reports_not_connected() {
    // The precondition is observable only here, above the query boundary.
    let transport = Arc::new(MockTransport::builder().build());
    let client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );

    match client.interrupt().await.unwrap_err() {
        InterruptError::NotConnected => {}
        other => panic!("expected NotConnected, got {other:?}"),
    }

    assert!(
        transport.written_messages().is_empty(),
        "a disconnected interrupt must not reach the transport"
    );
}

#[tokio::test]
async fn test_interrupt_reader_exit_releases_a_waiting_request() {
    // Half one of the registration race: the waiter is already registered when
    // the reader exits, so the reader's release sweep is what resolves it.
    // Closing the transport ends the read stream cleanly, with no stream error
    // — the exact path that used to hang forever.
    let transport = Arc::new(MockTransport::builder().build());

    let closer = {
        let transport = Arc::clone(&transport);
        tokio::spawn(async move {
            loop {
                let written = transport.written_messages_async().await;
                if written.iter().any(|message| {
                    message.parsed.as_ref().is_some_and(|parsed| {
                        parsed.pointer("/request/subtype").and_then(|v| v.as_str())
                            == Some("interrupt")
                    })
                }) {
                    transport.close().await.unwrap();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    let mut client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), client.interrupt())
        .await
        .expect("a reader exit must release the waiter rather than hang");

    closer.await.unwrap();

    match result.unwrap_err() {
        InterruptError::AcknowledgementUnavailable => {}
        other => panic!("expected AcknowledgementUnavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn test_interrupt_registered_after_reader_exit_reports_no_acknowledgement() {
    // Half two of the registration race, made deterministic by ordering: the
    // first interrupt returns only once the reader has exited, so the second
    // one necessarily registers *after* the release sweep and can only be
    // resolved by the exit flag it checks for itself.
    //
    // The assertion is sharp rather than tautological. Once the transport is
    // closed its `write` fails, so a second interrupt that reached the write
    // would report `SendFailed`. `AcknowledgementUnavailable` is therefore
    // reachable only by withdrawing at registration, before the write.
    let transport = Arc::new(MockTransport::builder().build());

    let closer = {
        let transport = Arc::clone(&transport);
        tokio::spawn(async move {
            loop {
                let written = transport.written_messages_async().await;
                if written.iter().any(|message| {
                    message.parsed.as_ref().is_some_and(|parsed| {
                        parsed.pointer("/request/subtype").and_then(|v| v.as_str())
                            == Some("interrupt")
                    })
                }) {
                    transport.close().await.unwrap();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    let mut client = ClaudeClient::with_transport(
        Arc::clone(&transport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let first = tokio::time::timeout(Duration::from_secs(5), client.interrupt())
        .await
        .expect("the first interrupt must be released by the reader exit");
    closer.await.unwrap();
    match first.unwrap_err() {
        InterruptError::AcknowledgementUnavailable => {}
        other => panic!(
            "expected the first interrupt to report AcknowledgementUnavailable, got {other:?}"
        ),
    }

    let writes_before = transport.written_messages().len();

    let second = tokio::time::timeout(Duration::from_secs(5), client.interrupt())
        .await
        .expect("a post-exit registration must not hang");

    match second.unwrap_err() {
        InterruptError::AcknowledgementUnavailable => {}
        other => panic!(
            "expected AcknowledgementUnavailable from the post-exit registration, got {other:?}"
        ),
    }

    assert_eq!(
        transport.written_messages().len(),
        writes_before,
        "the post-exit registration must withdraw before writing"
    );
}

/// A transport whose writes always fail and whose read stream never ends.
///
/// The reader task therefore stays alive, so the exit flag stays clear and the
/// write failure is the only outcome the interrupt path can reach. `MockTransport`
/// cannot isolate this site: its only write failure is the post-`close()` one,
/// and closing also ends the read stream, so which of the two failures wins
/// would be a race with the reader's exit rather than a fixed outcome.
struct WriteFailingTransport;

#[async_trait::async_trait]
impl Transport for WriteFailingTransport {
    async fn connect(&self) -> claude_agent_sdk_rs::Result<()> {
        Ok(())
    }

    async fn write(&self, _data: &str) -> claude_agent_sdk_rs::Result<()> {
        Err(ClaudeError::Transport("stdin is unavailable".to_string()))
    }

    fn read_messages(
        &self,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = claude_agent_sdk_rs::Result<serde_json::Value>> + Send + '_>,
    > {
        Box::pin(futures::stream::pending())
    }

    async fn close(&self) -> claude_agent_sdk_rs::Result<()> {
        Ok(())
    }

    fn is_ready(&self) -> bool {
        true
    }

    async fn end_input(&self) -> claude_agent_sdk_rs::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_interrupt_write_failure_reports_send_failed() {
    // `SendFailed` keeps the transport error whole and claims nothing about
    // delivery: body, newline, and flush are separately fallible.
    let mut client = ClaudeClient::with_transport(
        Arc::new(WriteFailingTransport) as Arc<dyn Transport>,
        ClaudeAgentOptions::default(),
    );
    client.connect_with_transport().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), client.interrupt())
        .await
        .expect("should not timeout");

    match result.unwrap_err() {
        InterruptError::SendFailed { source } => {
            assert!(
                source.to_string().contains("stdin is unavailable"),
                "the transport error must be preserved whole, got: {source}"
            );
        }
        other => panic!("expected SendFailed, got {other:?}"),
    }
}
