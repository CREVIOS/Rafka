#![forbid(unsafe_code)]

use rafka_protocol::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    VersionedCodec, SASL_AUTHENTICATE_MAX_VERSION, SASL_AUTHENTICATE_MIN_VERSION,
    SASL_HANDSHAKE_MAX_VERSION, SASL_HANDSHAKE_MIN_VERSION,
};
use rafka_protocol::ProtocolError;

fn assert_roundtrip<T>(message: &T, version: i16)
where
    T: VersionedCodec + core::fmt::Debug + PartialEq,
{
    let encoded = message.encode(version).expect("encode");
    let (decoded, read) = T::decode(version, &encoded).expect("decode");
    assert_eq!(decoded, *message);
    assert_eq!(read, encoded.len());
}

#[test]
fn sasl_handshake_roundtrip_all_versions() {
    for version in SASL_HANDSHAKE_MIN_VERSION..=SASL_HANDSHAKE_MAX_VERSION {
        let request = SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-256".to_string(),
        };
        assert_roundtrip(&request, version);
        let response = SaslHandshakeResponse {
            error_code: 0,
            mechanisms: vec![
                "PLAIN".to_string(),
                "SCRAM-SHA-256".to_string(),
                "SCRAM-SHA-512".to_string(),
            ],
        };
        assert_roundtrip(&response, version);
    }
}

#[test]
fn sasl_authenticate_roundtrip_all_versions() {
    for version in SASL_AUTHENTICATE_MIN_VERSION..=SASL_AUTHENTICATE_MAX_VERSION {
        let request = SaslAuthenticateRequest {
            auth_bytes: vec![0, b'u', 0, b'p'],
        };
        assert_roundtrip(&request, version);

        let response = SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: if version == 0 {
                Vec::new()
            } else {
                b"v=signature".to_vec()
            },
            session_lifetime_ms: (version >= 1).then_some(0),
        };
        assert_roundtrip(&response, version);
    }
}

#[test]
fn sasl_authenticate_skips_unknown_flexible_tag() {
    let response = SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: b"ok".to_vec(),
        session_lifetime_ms: Some(0),
    };
    let mut encoded = response.encode(2).expect("encode");
    assert_eq!(encoded.pop(), Some(0));
    encoded.extend([1, 7, 1, 0xff]);

    let (decoded, read) = SaslAuthenticateResponse::decode(2, &encoded).expect("decode");
    assert_eq!(decoded, response);
    assert_eq!(read, encoded.len());
}

#[test]
fn sasl_authenticate_truncated_payload_rejected() {
    let encoded = SaslAuthenticateRequest {
        auth_bytes: b"n,,n=user,r=nonce".to_vec(),
    }
    .encode(2)
    .expect("encode");
    for cut in 0..encoded.len() {
        let err = SaslAuthenticateRequest::decode(2, &encoded[..cut]).expect_err("must fail");
        assert_eq!(err, ProtocolError::Truncated);
    }
}
