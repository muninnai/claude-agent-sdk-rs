//! Full Query implementation with bidirectional control protocol

use dashmap::DashMap;
use futures::stream::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::oneshot;

use crate::errors::{ClaudeError, Result};
use crate::types::hooks::{HookCallback, HookContext, HookInput, HookMatcher};
use crate::types::mcp::McpSdkServerConfig;
use crate::types::interrupt::InterruptRequestAccepted;
use crate::types::permissions::{CanUseToolCallback, PermissionResult, ToolPermissionContext};

use super::transport::Transport;

/// Control request from SDK to CLI
#[allow(dead_code)]
#[derive(Debug, serde::Serialize)]
struct ControlRequest {
    #[serde(rename = "type")]
    type_: String,
    request_id: String,
    request: serde_json::Value,
}

/// Control response from CLI to SDK
#[derive(Debug, serde::Deserialize)]
struct ControlResponse {
    #[serde(rename = "type")]
    #[allow(dead_code)]
    type_: String,
    response: ControlResponseData,
}

#[derive(Debug, serde::Deserialize)]
struct ControlResponseData {
    subtype: String,
    request_id: String,
    #[serde(flatten)]
    data: serde_json::Value,
}

/// A correlated control response as handed to the waiter.
///
/// Carries the discriminator beside the complete flattened remainder that
/// callers already receive, so no sibling key is lost. Only `interrupt()`
/// interprets `subtype`; the generic helper continues to return `data` alone.
#[derive(Debug)]
struct RoutedControlResponse {
    subtype: String,
    data: serde_json::Value,
}

/// Why a routed control request produced no correlated response.
///
/// `Write` keeps the transport failure whole; delivery is indeterminate,
/// because the body, the newline, and the flush are separately fallible.
/// `NoAcknowledgement` means the waiter was released without an answer.
#[derive(Debug)]
enum RoutedSendFailure {
    Write(ClaudeError),
    NoAcknowledgement,
}

/// Failure of an interrupt request at the private query boundary.
///
/// The missing-query precondition is unrepresentable here: it can only be
/// observed by the public client before a query exists.
#[derive(Debug)]
pub(crate) enum InterruptQueryError {
    SendFailed { source: ClaudeError },
    ErrorResponse { message: String },
    AcknowledgementUnavailable,
    UnexpectedResponse { subtype: String },
}

/// Read the optional `still_queued` receipt out of a success payload.
///
/// `data` is the flattened remainder of the response object, so the receipt
/// sits under the nested `response` key. Returns `None` when the field is
/// absent, null, or not an array of strings.
///
/// Parsing is all-or-nothing: a mixed array yields `None`, never a partial
/// receipt. Dropping the non-string elements would report a *shorter* survivor
/// list as though it were the whole one, which is a stronger claim than the
/// wire supports. `None` never means the acknowledgement was missing.
fn parse_still_queued(data: &serde_json::Value) -> Option<Vec<String>> {
    data.pointer("/response/still_queued")?
        .as_array()?
        .iter()
        .map(|item| item.as_str().map(str::to_string))
        .collect()
}

/// Control request from CLI to SDK
#[derive(Debug, serde::Deserialize)]
struct IncomingControlRequest {
    #[serde(rename = "type")]
    #[allow(dead_code)]
    type_: String,
    request_id: String,
    request: serde_json::Value,
}

/// Full Query implementation with bidirectional control protocol
pub struct QueryFull {
    /// Transport for communication - uses &self methods via internal sync
    pub(crate) transport: Arc<dyn Transport>,
    /// Hook callbacks - concurrent access via DashMap
    hook_callbacks: Arc<DashMap<String, HookCallback>>,
    /// SDK MCP servers - concurrent access via DashMap
    sdk_mcp_servers: Arc<DashMap<String, McpSdkServerConfig>>,
    /// Tool permission callback - optional, for dynamic permission decisions
    can_use_tool: Option<CanUseToolCallback>,
    next_callback_id: Arc<AtomicU64>,
    request_counter: Arc<AtomicU64>,
    /// Pending control request responses - concurrent access via DashMap
    pending_responses: Arc<DashMap<String, oneshot::Sender<RoutedControlResponse>>>,
    /// Message sender - Option so start() can take ownership via .take()
    message_tx: Option<flume::Sender<serde_json::Value>>,
    /// Message receiver - cloneable without mutex thanks to flume
    pub(crate) message_rx: flume::Receiver<serde_json::Value>,
    /// Initialization result - set once during initialize(), read many times
    initialization_result: OnceLock<serde_json::Value>,
    /// Set once the response reader task has exited, by any route.
    ///
    /// A request registered concurrently with that exit would otherwise miss
    /// the reader's release sweep and wait forever, so registration re-checks
    /// this flag and withdraws its own entry when the reader is already gone.
    reader_has_exited: Arc<AtomicBool>,
}

impl QueryFull {
    /// Create a new Query
    pub fn new(transport: Box<dyn Transport>) -> Self {
        let (message_tx, message_rx) = flume::unbounded();

        Self {
            transport: Arc::from(transport),
            hook_callbacks: Arc::new(DashMap::new()),
            sdk_mcp_servers: Arc::new(DashMap::new()),
            can_use_tool: None,
            next_callback_id: Arc::new(AtomicU64::new(0)),
            request_counter: Arc::new(AtomicU64::new(0)),
            pending_responses: Arc::new(DashMap::new()),
            message_tx: Some(message_tx),
            message_rx,
            initialization_result: OnceLock::new(),
            reader_has_exited: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a new Query with a pre-existing Arc transport (for testing)
    #[cfg(feature = "testing")]
    pub fn new_with_transport(transport: Arc<dyn Transport>) -> Self {
        let (message_tx, message_rx) = flume::unbounded();

        Self {
            transport,
            hook_callbacks: Arc::new(DashMap::new()),
            sdk_mcp_servers: Arc::new(DashMap::new()),
            can_use_tool: None,
            next_callback_id: Arc::new(AtomicU64::new(0)),
            request_counter: Arc::new(AtomicU64::new(0)),
            pending_responses: Arc::new(DashMap::new()),
            message_tx: Some(message_tx),
            message_rx,
            initialization_result: OnceLock::new(),
            reader_has_exited: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set SDK MCP servers
    pub fn set_sdk_mcp_servers(&mut self, servers: HashMap<String, McpSdkServerConfig>) {
        self.sdk_mcp_servers.clear();
        for (name, config) in servers {
            self.sdk_mcp_servers.insert(name, config);
        }
    }

    /// Set tool permission callback
    pub fn set_can_use_tool(&mut self, callback: Option<CanUseToolCallback>) {
        self.can_use_tool = callback;
    }

    /// Initialize with hooks
    pub async fn initialize(
        &self,
        hooks: Option<HashMap<String, Vec<HookMatcher>>>,
    ) -> Result<serde_json::Value> {
        // Build hooks configuration
        let mut hooks_config: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

        if let Some(hooks_map) = hooks {
            for (event, matchers) in hooks_map {
                let mut event_matchers = Vec::new();

                for matcher in matchers {
                    let mut callback_ids = Vec::new();

                    for callback in matcher.hooks {
                        let callback_id = format!(
                            "hook_{}",
                            self.next_callback_id.fetch_add(1, Ordering::SeqCst)
                        );
                        self.hook_callbacks.insert(callback_id.clone(), callback);
                        callback_ids.push(callback_id);
                    }

                    let mut matcher_json = json!({
                        "matcher": matcher.matcher,
                        "hookCallbackIds": callback_ids
                    });

                    // Add timeout if specified
                    if let Some(timeout) = matcher.timeout {
                        matcher_json["timeout"] = json!(timeout);
                    }

                    event_matchers.push(matcher_json);
                }

                hooks_config.insert(event, event_matchers);
            }
        }

        // Send initialize request
        let request = json!({
            "subtype": "initialize",
            "hooks": if hooks_config.is_empty() { json!(null) } else { json!(hooks_config) }
        });

        let response = self.send_control_request(request).await?;

        // Store initialization result for get_server_info() (set once, read many)
        let _ = self.initialization_result.set(response.clone());

        Ok(response)
    }

    /// Start reading messages in background
    ///
    /// Returns a receiver that signals when the background task completes.
    /// The caller should store this and await it during disconnect.
    pub async fn start(&mut self) -> Result<oneshot::Receiver<()>> {
        let transport = Arc::clone(&self.transport);
        let transport_for_hooks = Arc::clone(&self.transport);
        let hook_callbacks = Arc::clone(&self.hook_callbacks);
        let sdk_mcp_servers = Arc::clone(&self.sdk_mcp_servers);
        let can_use_tool = self.can_use_tool.clone();
        let pending_responses = Arc::clone(&self.pending_responses);
        let reader_has_exited = Arc::clone(&self.reader_has_exited);
        // Take ownership of message_tx
        let message_tx = self
            .message_tx
            .take()
            .expect("start() must only be called once");

        // Create a channel to signal when background task is ready
        let (ready_tx, ready_rx) = oneshot::channel();

        // Create a channel to signal when background task completes
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            // No lock needed - Transport uses &self methods with internal sync
            let mut stream = transport.read_messages();

            // Signal that we're ready to receive messages
            let _ = ready_tx.send(());

            let mut stream_error: Option<String> = None;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        let msg_type = message.get("type").and_then(|v| v.as_str());

                        match msg_type {
                            Some("control_response") => {
                                // Handle control response
                                if let Ok(response) =
                                    serde_json::from_value::<ControlResponse>(message.clone())
                                {
                                    // DashMap remove returns Option<(K, V)>
                                    if let Some((_, tx)) =
                                        pending_responses.remove(&response.response.request_id)
                                    {
                                        let _ = tx.send(RoutedControlResponse {
                                            subtype: response.response.subtype,
                                            data: response.response.data,
                                        });
                                    }
                                }
                            }
                            Some("control_request") => {
                                // Handle incoming control request (e.g., hook callback, MCP message, permission)
                                if let Ok(request) = serde_json::from_value::<IncomingControlRequest>(
                                    message.clone(),
                                ) {
                                    let transport_clone = Arc::clone(&transport_for_hooks);
                                    let hook_callbacks_clone = Arc::clone(&hook_callbacks);
                                    let sdk_mcp_servers_clone = Arc::clone(&sdk_mcp_servers);
                                    let can_use_tool_clone = can_use_tool.clone();

                                    tokio::spawn(async move {
                                        if let Err(e) = Self::handle_control_request(
                                            request,
                                            transport_clone,
                                            hook_callbacks_clone,
                                            sdk_mcp_servers_clone,
                                            can_use_tool_clone,
                                        )
                                        .await
                                        {
                                            eprintln!("Error handling control request: {}", e);
                                        }
                                    });
                                }
                            }
                            _ => {
                                // Regular message - send to stream
                                let _ = message_tx.send(message);
                            }
                        }
                    }
                    Err(e) => {
                        // Store error for sentinel message
                        stream_error = Some(e.to_string());
                        break;
                    }
                }
            }

            // Release pending control requests on *every* reader exit, not only
            // on error. A clean EOF ends this loop without setting
            // `stream_error`, and a waiter whose sender is never dropped would
            // otherwise block forever.
            //
            // Dropping each sender resolves its waiter with a receive error,
            // which the interrupt path reports as an unavailable
            // acknowledgement rather than as any claim about effect.
            reader_has_exited.store(true, Ordering::SeqCst);
            pending_responses.clear();

            // Send error sentinel if there was an error
            if let Some(ref error) = stream_error {
                let _ = message_tx.send(json!({"type": "error", "error": error}));
            }

            // Always send end sentinel
            let _ = message_tx.send(json!({"type": "end"}));

            // Signal that background task has completed
            let _ = shutdown_tx.send(());
        });

        // Wait for background task to be ready before returning
        ready_rx
            .await
            .map_err(|_| ClaudeError::Transport("Background task failed to start".to_string()))?;

        Ok(shutdown_rx)
    }

    /// Handle incoming control request from CLI
    async fn handle_control_request(
        request: IncomingControlRequest,
        transport: Arc<dyn Transport>,
        hook_callbacks: Arc<DashMap<String, HookCallback>>,
        sdk_mcp_servers: Arc<DashMap<String, McpSdkServerConfig>>,
        can_use_tool: Option<CanUseToolCallback>,
    ) -> Result<()> {
        let request_id = request.request_id;
        let request_data = request.request;

        let subtype = request_data
            .get("subtype")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ClaudeError::ControlProtocol("Missing subtype".to_string()))?;

        let response_data: serde_json::Value = match subtype {
            "can_use_tool" => {
                // Handle tool permission request
                let callback = can_use_tool.ok_or_else(|| {
                    ClaudeError::ControlProtocol(
                        "can_use_tool callback is not provided".to_string(),
                    )
                })?;

                let tool_name = request_data
                    .get("tool_name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ClaudeError::ControlProtocol("Missing tool_name".to_string()))?
                    .to_string();

                let original_input = request_data.get("input").cloned().unwrap_or(json!({}));

                // Parse permission suggestions if present
                let suggestions = request_data
                    .get("permission_suggestions")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();

                let context = ToolPermissionContext {
                    signal: None,
                    suggestions,
                };

                // Call the permission callback
                let result = callback(tool_name, original_input.clone(), context).await;

                // Convert PermissionResult to response format
                match result {
                    PermissionResult::Allow(allow) => {
                        let mut response = json!({
                            "behavior": "allow",
                            "updatedInput": allow.updated_input.unwrap_or(original_input)
                        });
                        if let Some(updated_permissions) = allow.updated_permissions {
                            response["updatedPermissions"] =
                                serde_json::to_value(updated_permissions).unwrap_or(json!([]));
                        }
                        response
                    }
                    PermissionResult::Deny(deny) => {
                        let mut response = json!({
                            "behavior": "deny",
                            "message": deny.message
                        });
                        if deny.interrupt {
                            response["interrupt"] = json!(true);
                        }
                        response
                    }
                }
            }
            "hook_callback" => {
                // Execute hook callback
                let callback_id = request_data
                    .get("callback_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        ClaudeError::ControlProtocol("Missing callback_id".to_string())
                    })?;

                // Clone the callback Arc to release the DashMap guard before async call
                let callback = hook_callbacks
                    .get(callback_id)
                    .map(|r| r.clone())
                    .ok_or_else(|| {
                        ClaudeError::ControlProtocol(format!(
                            "Hook callback not found: {}",
                            callback_id
                        ))
                    })?;

                // Parse hook input
                let input_json = request_data.get("input").cloned().unwrap_or(json!({}));
                let hook_input: HookInput = serde_json::from_value(input_json).map_err(|e| {
                    ClaudeError::ControlProtocol(format!("Failed to parse hook input: {}", e))
                })?;

                let tool_use_id = request_data
                    .get("tool_use_id")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let context = HookContext::default();

                // Call the hook
                let hook_output = callback(hook_input, tool_use_id, context).await;

                // Convert to JSON
                serde_json::to_value(&hook_output).map_err(|e| {
                    ClaudeError::ControlProtocol(format!("Failed to serialize hook output: {}", e))
                })?
            }
            "mcp_message" => {
                // Handle SDK MCP message
                let server_name = request_data
                    .get("server_name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        ClaudeError::ControlProtocol(
                            "Missing server_name for mcp_message".to_string(),
                        )
                    })?;

                let mcp_message = request_data.get("message").ok_or_else(|| {
                    ClaudeError::ControlProtocol("Missing message for mcp_message".to_string())
                })?;

                let mcp_response =
                    Self::handle_sdk_mcp_request(sdk_mcp_servers, server_name, mcp_message.clone())
                        .await?;

                json!({"mcp_response": mcp_response})
            }
            _ => {
                return Err(ClaudeError::ControlProtocol(format!(
                    "Unsupported control request subtype: {}",
                    subtype
                )));
            }
        };

        // Send success response
        let response = json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": response_data
            }
        });

        let response_str = serde_json::to_string(&response)
            .map_err(|e| ClaudeError::Transport(format!("Failed to serialize response: {}", e)))?;

        // Write via transport - stdin/stdout have separate locks, no deadlock
        transport.write(&response_str).await?;

        Ok(())
    }

    /// Send a control request and hand back the correlated response with its
    /// discriminator intact.
    ///
    /// The two failure modes are kept apart here so that each caller classifies
    /// from a site where the distinction is still known, rather than
    /// reconstructing it later from a rendered message.
    async fn send_control_request_routed(
        &self,
        request: serde_json::Value,
    ) -> std::result::Result<RoutedControlResponse, RoutedSendFailure> {
        let request_id = format!(
            "req_{}_{}",
            self.request_counter.fetch_add(1, Ordering::SeqCst),
            uuid::Uuid::new_v4().simple()
        );

        // Create oneshot channel for response
        let (tx, rx) = oneshot::channel();
        self.pending_responses.insert(request_id.clone(), tx);

        // Close the registration race. The reader may have run its release
        // sweep between exiting and this insertion, in which case nothing will
        // ever resolve this waiter. Withdraw the entry instead of waiting
        // forever.
        if self.reader_has_exited.load(Ordering::SeqCst) {
            self.pending_responses.remove(&request_id);
            return Err(RoutedSendFailure::NoAcknowledgement);
        }

        // Build and send request
        let control_request = json!({
            "type": "control_request",
            "request_id": request_id,
            "request": request
        });

        let request_str = serde_json::to_string(&control_request).map_err(|e| {
            RoutedSendFailure::Write(ClaudeError::Transport(format!(
                "Failed to serialize request: {}",
                e
            )))
        })?;

        // Write via transport - stdin/stdout have separate locks, no deadlock
        self.transport
            .write(&request_str)
            .await
            .map_err(RoutedSendFailure::Write)?;

        // Wait for response
        rx.await.map_err(|_| RoutedSendFailure::NoAcknowledgement)
    }

    /// Send control request to CLI
    async fn send_control_request(&self, request: serde_json::Value) -> Result<serde_json::Value> {
        self.send_control_request_routed(request)
            .await
            .map(|routed| routed.data)
            .map_err(|failure| match failure {
                RoutedSendFailure::Write(error) => error,
                RoutedSendFailure::NoAcknowledgement => ClaudeError::ControlProtocol(
                    "Control request response channel closed".to_string(),
                ),
            })
    }

    /// Receive messages
    #[allow(dead_code)]
    pub async fn receive_messages(&self) -> Vec<serde_json::Value> {
        let mut messages = Vec::new();
        let rx = self.message_rx.clone();

        while let Ok(message) = rx.recv_async().await {
            messages.push(message);
        }

        messages
    }

    /// Send an interrupt request to Claude Code and report its outcome.
    ///
    /// Returning `Ok` means Claude Code *accepted the request*. It does not mean
    /// a turn stopped, and it does not mean a turn was running: an interrupt
    /// sent during an active turn and one sent while nothing is running receive
    /// the same success envelope. Evidence that a turn was actually interrupted
    /// arrives separately, on the message stream.
    pub(crate) async fn interrupt(
        &self,
    ) -> std::result::Result<InterruptRequestAccepted, InterruptQueryError> {
        let request = json!({
            "subtype": "interrupt"
        });

        let routed = self
            .send_control_request_routed(request)
            .await
            .map_err(|failure| match failure {
                RoutedSendFailure::Write(source) => InterruptQueryError::SendFailed { source },
                RoutedSendFailure::NoAcknowledgement => {
                    InterruptQueryError::AcknowledgementUnavailable
                }
            })?;

        // The fold is total over correlated responses: anything the
        // deserializer admits but this match cannot interpret is reported as
        // unusable rather than as acceptance or as a failure to deliver.
        match routed.subtype.as_str() {
            "success" => Ok(InterruptRequestAccepted {
                still_queued: parse_still_queued(&routed.data),
            }),
            "error" => match routed.data.get("error").and_then(|value| value.as_str()) {
                Some(message) => Err(InterruptQueryError::ErrorResponse {
                    message: message.to_string(),
                }),
                None => Err(InterruptQueryError::UnexpectedResponse {
                    subtype: routed.subtype,
                }),
            },
            _ => Err(InterruptQueryError::UnexpectedResponse {
                subtype: routed.subtype,
            }),
        }
    }

    /// Change permission mode dynamically
    ///
    /// Returns the confirmed effective permission mode from Claude Code's response.
    /// Returns an error if Claude Code rejects the mode change (e.g., runtime bypass
    /// requires the session to have been launched with bypass capability).
    pub async fn set_permission_mode(
        &self,
        mode: crate::types::config::PermissionMode,
    ) -> Result<crate::types::config::PermissionMode> {
        let mode_str = match mode {
            crate::types::config::PermissionMode::Default => "default",
            crate::types::config::PermissionMode::AcceptEdits => "acceptEdits",
            crate::types::config::PermissionMode::Plan => "plan",
            crate::types::config::PermissionMode::BypassPermissions => "bypassPermissions",
        };

        let request = json!({
            "subtype": "set_permission_mode",
            "mode": mode_str
        });

        let response = self.send_control_request(request).await?;

        if let Some(error) = response.get("error").and_then(|v| v.as_str()) {
            return Err(ClaudeError::ControlProtocol(error.to_string()));
        }

        let mode_value = response.pointer("/response/mode").ok_or_else(|| {
            ClaudeError::ControlProtocol(
                "set_permission_mode response missing response.mode".to_string(),
            )
        })?;

        serde_json::from_value::<crate::types::config::PermissionMode>(mode_value.clone()).map_err(
            |e| {
                ClaudeError::ControlProtocol(format!(
                    "set_permission_mode response contained invalid mode: {e}"
                ))
            },
        )
    }

    /// Change AI model dynamically
    pub async fn set_model(&self, model: Option<&str>) -> Result<()> {
        let request = json!({
            "subtype": "set_model",
            "model": model
        });

        self.send_control_request(request).await?;
        Ok(())
    }

    /// Rewind tracked files to their state at a specific user message.
    ///
    /// Requires:
    /// - `enable_file_checkpointing=true` to track file changes
    /// - `extra_args={"replay-user-messages": None}` to receive UserMessage
    ///   objects with `uuid` in the response stream
    ///
    /// # Arguments
    /// * `user_message_id` - UUID of the user message to rewind to. This should be
    ///   the `uuid` field from a `UserMessage` received during the conversation.
    pub async fn rewind_files(&self, user_message_id: &str) -> Result<()> {
        let request = json!({
            "subtype": "rewind_files",
            "user_message_id": user_message_id
        });

        self.send_control_request(request).await?;
        Ok(())
    }

    /// Get server initialization info
    ///
    /// Returns the initialization result that was obtained during connect().
    /// This includes information about available commands, output styles, and server capabilities.
    /// This is lock-free since initialization_result uses OnceLock.
    pub fn get_initialization_result(&self) -> Option<serde_json::Value> {
        self.initialization_result.get().cloned()
    }

    /// Handle SDK MCP request by routing to the appropriate server
    async fn handle_sdk_mcp_request(
        sdk_mcp_servers: Arc<DashMap<String, McpSdkServerConfig>>,
        server_name: &str,
        message: serde_json::Value,
    ) -> Result<serde_json::Value> {
        // Clone the server config to release the DashMap guard before async call
        let server_config = sdk_mcp_servers
            .get(server_name)
            .map(|r| r.clone())
            .ok_or_else(|| {
                ClaudeError::ControlProtocol(format!("SDK MCP server not found: {}", server_name))
            })?;

        // Call the server's handle_message method
        server_config
            .instance
            .handle_message(message)
            .await
            .map_err(|e| ClaudeError::ControlProtocol(format!("MCP server error: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::permissions::{PermissionResultAllow, PermissionResultDeny};
    use async_trait::async_trait;
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Simple mock transport for testing - matches Python's MockTransport pattern
    struct MockTransport {
        written_messages: Mutex<Vec<String>>,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                written_messages: Mutex::new(Vec::new()),
            }
        }

        fn written_messages(&self) -> Vec<String> {
            self.written_messages.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn connect(&self) -> Result<()> {
            Ok(())
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }

        async fn write(&self, data: &str) -> Result<()> {
            self.written_messages.lock().unwrap().push(data.to_string());
            Ok(())
        }

        async fn end_input(&self) -> Result<()> {
            Ok(())
        }

        fn read_messages(&self) -> futures::stream::BoxStream<'static, Result<serde_json::Value>> {
            futures::stream::empty().boxed()
        }

        fn is_ready(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_permission_callback_allow() {
        // Track if callback was invoked
        let callback_invoked = Arc::new(AtomicBool::new(false));
        let callback_invoked_clone = Arc::clone(&callback_invoked);

        let allow_callback: CanUseToolCallback = Arc::new(
            move |tool_name: String,
                  tool_input: serde_json::Value,
                  _context: ToolPermissionContext|
                  -> BoxFuture<'static, PermissionResult> {
                let invoked = Arc::clone(&callback_invoked_clone);
                async move {
                    invoked.store(true, Ordering::SeqCst);
                    assert_eq!(tool_name, "TestTool");
                    assert_eq!(tool_input["param"], "value");
                    PermissionResult::Allow(PermissionResultAllow::default())
                }
                .boxed()
            },
        );

        let transport = Arc::new(MockTransport::new());

        // Simulate control request - matches Python test structure
        let request = IncomingControlRequest {
            type_: "control_request".to_string(),
            request_id: "test-1".to_string(),
            request: json!({
                "subtype": "can_use_tool",
                "tool_name": "TestTool",
                "input": {"param": "value"},
                "permission_suggestions": []
            }),
        };

        QueryFull::handle_control_request(
            request,
            transport.clone(),
            Arc::new(DashMap::new()),
            Arc::new(DashMap::new()),
            Some(allow_callback),
        )
        .await
        .unwrap();

        // Check callback was invoked
        assert!(
            callback_invoked.load(Ordering::SeqCst),
            "Permission callback should have been invoked"
        );

        // Check response was sent
        let written = transport.written_messages();
        assert_eq!(written.len(), 1);
        assert!(written[0].contains(r#""behavior":"allow""#));
    }

    #[tokio::test]
    async fn test_permission_callback_deny() {
        let deny_callback: CanUseToolCallback = Arc::new(
            move |_tool_name: String,
                  _tool_input: serde_json::Value,
                  _context: ToolPermissionContext|
                  -> BoxFuture<'static, PermissionResult> {
                async move {
                    PermissionResult::Deny(PermissionResultDeny {
                        message: "Security policy violation".to_string(),
                        interrupt: false,
                    })
                }
                .boxed()
            },
        );

        let transport = Arc::new(MockTransport::new());

        let request = IncomingControlRequest {
            type_: "control_request".to_string(),
            request_id: "test-2".to_string(),
            request: json!({
                "subtype": "can_use_tool",
                "tool_name": "DangerousTool",
                "input": {"command": "rm -rf /"},
                "permission_suggestions": ["deny"]
            }),
        };

        QueryFull::handle_control_request(
            request,
            transport.clone(),
            Arc::new(DashMap::new()),
            Arc::new(DashMap::new()),
            Some(deny_callback),
        )
        .await
        .unwrap();

        // Check response
        let written = transport.written_messages();
        assert_eq!(written.len(), 1);
        assert!(written[0].contains(r#""behavior":"deny""#));
        assert!(written[0].contains("Security policy violation"));
    }

    #[tokio::test]
    async fn test_permission_callback_input_modification() {
        let modify_callback: CanUseToolCallback = Arc::new(
            move |_tool_name: String,
                  tool_input: serde_json::Value,
                  _context: ToolPermissionContext|
                  -> BoxFuture<'static, PermissionResult> {
                async move {
                    // Modify the input to add safety flag
                    let mut modified_input = tool_input.clone();
                    modified_input["safe_mode"] = json!(true);
                    PermissionResult::Allow(PermissionResultAllow {
                        updated_input: Some(modified_input),
                        updated_permissions: None,
                    })
                }
                .boxed()
            },
        );

        let transport = Arc::new(MockTransport::new());

        let request = IncomingControlRequest {
            type_: "control_request".to_string(),
            request_id: "test-3".to_string(),
            request: json!({
                "subtype": "can_use_tool",
                "tool_name": "WriteTool",
                "input": {"file_path": "/etc/passwd"},
                "permission_suggestions": []
            }),
        };

        QueryFull::handle_control_request(
            request,
            transport.clone(),
            Arc::new(DashMap::new()),
            Arc::new(DashMap::new()),
            Some(modify_callback),
        )
        .await
        .unwrap();

        // Check response includes modified input
        let written = transport.written_messages();
        assert_eq!(written.len(), 1);
        assert!(written[0].contains(r#""behavior":"allow""#));
        assert!(written[0].contains(r#""safe_mode":true"#));
    }

    #[tokio::test]
    async fn test_permission_callback_deny_with_interrupt() {
        let callback: CanUseToolCallback = Arc::new(
            move |_tool_name: String,
                  _tool_input: serde_json::Value,
                  _context: ToolPermissionContext|
                  -> BoxFuture<'static, PermissionResult> {
                async move {
                    PermissionResult::Deny(PermissionResultDeny {
                        message: "Critical security violation".to_string(),
                        interrupt: true,
                    })
                }
                .boxed()
            },
        );

        let transport = Arc::new(MockTransport::new());

        let request = IncomingControlRequest {
            type_: "control_request".to_string(),
            request_id: "test-4".to_string(),
            request: json!({
                "subtype": "can_use_tool",
                "tool_name": "CriticalTool",
                "input": {},
                "permission_suggestions": []
            }),
        };

        QueryFull::handle_control_request(
            request,
            transport.clone(),
            Arc::new(DashMap::new()),
            Arc::new(DashMap::new()),
            Some(callback),
        )
        .await
        .unwrap();

        // Check response includes interrupt flag
        let written = transport.written_messages();
        assert_eq!(written.len(), 1);
        assert!(written[0].contains(r#""behavior":"deny""#));
        assert!(written[0].contains(r#""interrupt":true"#));
    }

    #[tokio::test]
    async fn test_permission_callback_not_provided() {
        let transport = Arc::new(MockTransport::new());

        let request = IncomingControlRequest {
            type_: "control_request".to_string(),
            request_id: "test-5".to_string(),
            request: json!({
                "subtype": "can_use_tool",
                "tool_name": "TestTool",
                "input": {},
                "permission_suggestions": []
            }),
        };

        // Should error when callback is not provided
        let result = QueryFull::handle_control_request(
            request,
            transport,
            Arc::new(DashMap::new()),
            Arc::new(DashMap::new()),
            None, // No callback
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("can_use_tool callback is not provided")
        );
    }
}
