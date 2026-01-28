//! This header contains the definitions for the File Transfer over TCP (FTCP)
//!
//! Contains Enums like Command, and the protocol struct FTCP
//!
pub const CMD_LEN: usize = 4;

#[derive(Clone, Copy)]
enum Command {
    List,
    Get,
    Send,
    Okay,
    Err,
}

impl Command {
    pub fn to_bytes(&self) -> [u8; CMD_LEN] {
        match self {
            Command::List => *b"LIST",
            Command::Get => *b"GET ",
            Command::Send => *b"SEND",
            Command::Okay => *b"OKAY",
            Command::Err => *b"ERR ",
            // *NOTE - Asterisk is used to dereference the value and provide the copied value instead of a slice
        }
    }
    pub fn from_bytes(bytes: [u8; CMD_LEN]) -> Result<Self, String> {
        match &bytes {
            b"LIST" => Ok(Command::List),
            b"GET " => Ok(Command::Get),
            b"SEND" => Ok(Command::Send),
            b"OKAY" => Ok(Command::Okay),
            b"ERR " => Ok(Command::Err),
            _ => Err("Unknown command".into()),
        }
    }
}

struct Ftcp {
    cmd: Command,
    len: u32,
    payload: Vec<u8>,
}

impl Ftcp {
    pub fn new(cmd: Command, payload: Vec<u8>) -> Self {
        let len = payload.len() as u32;
        Self { cmd, len, payload }
    }
}
