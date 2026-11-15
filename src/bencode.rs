//! Bencode (BEP 3) decoding and encoding.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bencode: {}", self.0)
    }
}

impl std::error::Error for Error {}

const MAX_DEPTH: usize = 64;

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Dict(d) => d.get(key.as_bytes()),
            _ => None,
        }
    }
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }
    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        match self {
            Value::Dict(d) => Some(d),
            _ => None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Int(i) => {
                out.push(b'i');
                out.extend_from_slice(i.to_string().as_bytes());
                out.push(b'e');
            }
            Value::Bytes(b) => {
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(b);
            }
            Value::List(l) => {
                out.push(b'l');
                for v in l {
                    v.encode_into(out);
                }
                out.push(b'e');
            }
            Value::Dict(d) => {
                out.push(b'd');
                for (k, v) in d {
                    out.extend_from_slice(k.len().to_string().as_bytes());
                    out.push(b':');
                    out.extend_from_slice(k);
                    v.encode_into(out);
                }
                out.push(b'e');
            }
        }
    }
}

/// Small builder helpers.
pub fn bytes(b: impl Into<Vec<u8>>) -> Value {
    Value::Bytes(b.into())
}
pub fn int(i: i64) -> Value {
    Value::Int(i)
}
pub fn dict<const N: usize>(entries: [(&str, Value); N]) -> Value {
    Value::Dict(entries.into_iter().map(|(k, v)| (k.as_bytes().to_vec(), v)).collect())
}

/// Decode one value; returns it plus the number of bytes consumed.
pub fn decode_prefix(input: &[u8]) -> Result<(Value, usize), Error> {
    let mut p = Parser { input, pos: 0, info_span: None };
    let v = p.value(0)?;
    Ok((v, p.pos))
}

/// Decode exactly one value spanning the whole input.
pub fn decode(input: &[u8]) -> Result<Value, Error> {
    let (v, used) = decode_prefix(input)?;
    if used != input.len() {
        return Err(Error("trailing data".into()));
    }
    Ok(v)
}

/// Decode a top-level dict and also return the raw byte span of its `info` value
/// (needed to compute the info hash exactly as encoded).
pub fn decode_with_info_span(input: &[u8]) -> Result<(Value, Option<(usize, usize)>), Error> {
    let mut p = Parser { input, pos: 0, info_span: None };
    let v = p.value(0)?;
    Ok((v, p.info_span))
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
    info_span: Option<(usize, usize)>,
}

impl Parser<'_> {
    fn peek(&self) -> Result<u8, Error> {
        self.input.get(self.pos).copied().ok_or_else(|| Error("unexpected end".into()))
    }

    fn value(&mut self, depth: usize) -> Result<Value, Error> {
        if depth > MAX_DEPTH {
            return Err(Error("nesting too deep".into()));
        }
        match self.peek()? {
            b'i' => {
                self.pos += 1;
                let end = self.find(b'e')?;
                let s = std::str::from_utf8(&self.input[self.pos..end]).map_err(|_| Error("bad int".into()))?;
                if s.is_empty() || (s.len() > 1 && (s.starts_with('0') || s.starts_with("-0"))) {
                    return Err(Error("non-canonical int".into()));
                }
                let i = s.parse::<i64>().map_err(|_| Error("bad int".into()))?;
                self.pos = end + 1;
                Ok(Value::Int(i))
            }
            b'l' => {
                self.pos += 1;
                let mut items = Vec::new();
                while self.peek()? != b'e' {
                    items.push(self.value(depth + 1)?);
                }
                self.pos += 1;
                Ok(Value::List(items))
            }
            b'd' => {
                self.pos += 1;
                let mut map = BTreeMap::new();
                while self.peek()? != b'e' {
                    let key = self.byte_string()?;
                    let start = self.pos;
                    let v = self.value(depth + 1)?;
                    if depth == 0 && key == b"info" {
                        self.info_span = Some((start, self.pos));
                    }
                    map.insert(key, v);
                }
                self.pos += 1;
                Ok(Value::Dict(map))
            }
            b'0'..=b'9' => Ok(Value::Bytes(self.byte_string()?)),
            c => Err(Error(format!("unexpected byte {c:#x}"))),
        }
    }

    fn find(&self, c: u8) -> Result<usize, Error> {
        self.input[self.pos..].iter().position(|&b| b == c).map(|i| self.pos + i).ok_or_else(|| Error("unterminated".into()))
    }

    fn byte_string(&mut self) -> Result<Vec<u8>, Error> {
        let colon = self.find(b':')?;
        let len_s = std::str::from_utf8(&self.input[self.pos..colon]).map_err(|_| Error("bad length".into()))?;
        let len: usize = len_s.parse().map_err(|_| Error("bad length".into()))?;
        let start = colon + 1;
        let end = start.checked_add(len).ok_or_else(|| Error("length overflow".into()))?;
        if end > self.input.len() {
            return Err(Error("string past end".into()));
        }
        self.pos = end;
        Ok(self.input[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let v = dict([("a", int(-3)), ("b", bytes(&b"spam"[..])), ("c", Value::List(vec![int(1), bytes(&b""[..])]))]);
        let enc = v.encode();
        assert_eq!(enc, b"d1:ai-3e1:b4:spam1:cli1e0:ee");
        assert_eq!(decode(&enc).unwrap(), v);
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode(b"i01e").is_err());
        assert!(decode(b"5:abc").is_err());
        assert!(decode(b"l").is_err());
        assert!(decode(b"i1ei2e").is_err());
        let deep = "l".repeat(200) + &"e".repeat(200);
        assert!(decode(deep.as_bytes()).is_err());
    }

    #[test]
    fn info_span() {
        let input = b"d8:announce3:url4:infod4:name1:xee";
        let (_, span) = decode_with_info_span(input).unwrap();
        let (s, e) = span.unwrap();
        assert_eq!(&input[s..e], b"d4:name1:xe");
    }
}
