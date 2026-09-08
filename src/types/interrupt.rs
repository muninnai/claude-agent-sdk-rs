//! Outcome types for an interrupt request.
//!
//! An interrupt control request is acknowledged by Claude Code when the request
//! is *accepted*. Acceptance is not a stop: a request sent during an active turn
//! and one sent while nothing is running receive the same success envelope, and
//! the two cases differ only afterwards on the message stream. Nothing in this
//! module reports that a turn stopped, or distinguishes idle from active work.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::errors::ClaudeError;

/// Claude Code accepted an interrupt request.
///
/// This says the request was accepted, and says nothing about whether a turn was
/// running or whether one stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterruptRequestAccepted {
    /// UUIDs of covered queued messages that survived the interrupt, when the
    /// CLI sent a receipt.
    ///
    /// `None` means no usable receipt was recovered — the field was absent,
    /// null, or unrecognized — while the acknowledgement itself was still
    /// received. An empty vector means no covered queued survivor was reported.
    /// No value says whether a turn was running or stopped.
    pub still_queued: Option<Vec<String>>,
}

/// An interrupt request produced no acknowledgement of acceptance.
///
/// No variant claims that a turn was or was not stopped.
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum InterruptError {
    /// The client held no query, so nothing could be sent.
    #[error("client is not connected")]
    NotConnected,

    /// Writing the request to the transport failed.
    ///
    /// Delivery is indeterminate: the body, the newline, and the flush are
    /// separately fallible, so a failure can occur after the framed request has
    /// already been written.
    #[error("interrupt send failed: {source}")]
    SendFailed {
        /// The transport failure that occurred.
        #[source]
        source: ClaudeError,
    },

    /// Claude Code answered with a correlated `error` envelope.
    ///
    /// The protocol documents this as a request that failed or was rejected;
    /// this variant does not assert which.
    #[error("interrupt returned an error response: {message}")]
    ErrorResponse {
        /// The message carried by the error envelope.
        message: String,
    },

    /// The response reader exited before answering.
    #[error("no acknowledgement was obtained for the interrupt")]
    AcknowledgementUnavailable,

    /// A correlated response arrived that could not be interpreted.
    ///
    /// Either its subtype was neither `success` nor `error`, or it was an
    /// `error` envelope whose `error` field was absent or not a string.
    #[error("interrupt received an unusable response with subtype {subtype}")]
    UnexpectedResponse {
        /// The subtype carried by the unusable response.
        subtype: String,
    },
}
