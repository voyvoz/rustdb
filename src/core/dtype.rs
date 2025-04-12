use std::io::{self};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum DataType {
    String(String),
    Int(i32),
    Float(f64),
}

impl DataType {
    pub fn from_str(s: &str) -> Self {
        let str = s.trim();
        if let Ok(i) = str.parse::<i32>() {
            DataType::Int(i)
        } else if let Ok(f) = str.parse::<f64>() {
            DataType::Float(f)
        } else {
            DataType::String(str.to_string())
        }
    }

    pub fn to_str(&self) -> String {
        match self {
            DataType::Int(i) => format!("{}", i),
            DataType::Float(f) => format!("{:.6}", f), // Limit precision to avoid floating-point comparison issues.
            DataType::String(s) => format!("{}", s),
        }
    }

    pub fn slen(&self) -> usize {
        match self {
            DataType::Int(_) => self.dlen() + 1,
            DataType::Float(_) => self.dlen() + 1, 
            DataType::String(_) => self.dlen() + 1,
        }
    }

    pub fn dlen(&self) -> usize {
        match self {
            DataType::Int(_) => std::mem::size_of::<i32>(),
            DataType::Float(_) => std::mem::size_of::<f64>(), 
            DataType::String(s) => s.len(),
        }
    }

    pub fn to_json(&self) -> String {
        match self {
            DataType::Int(_) => "\"Integer\"".to_string(),
            DataType::Float(_) => "\"Float\"".to_string(),
            DataType::String(_) => "\"String\"".to_string(),
        }
    }
}

impl Eq for DataType {}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            DataType::String(s) => s.hash(state),
            DataType::Int(i) => i.hash(state),
            DataType::Float(f) => {
                // Hash the bit representation of the float
                let bits = f.to_bits();
                bits.hash(state);
            }
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataType::String(s) => write!(f, "{}", s),
            DataType::Int(i) => write!(f, "{}", i),
            DataType::Float(fl) => write!(f, "{}", fl),
        }
    }
}

pub fn serialize_data_types(data_types: &[DataType]) -> io::Result<Vec<u8>> {
    // Example serialization function
    let mut bytes = Vec::new();
    for data_type in data_types {
        match data_type {
            DataType::String(s) => {
                bytes.push(0); // '0' prefix for String
                bytes.extend(s.len().to_be_bytes());
                bytes.extend(s.as_bytes());
            },
            DataType::Int(i) => {
                bytes.push(1); // '1' prefix for Int
                bytes.extend(i.to_be_bytes());
            },
            DataType::Float(f) => {
                bytes.push(2); // '2' prefix for Float
                bytes.extend(f.to_be_bytes());
            },
        }
    }
    Ok(bytes)
}

pub fn deserialize_data_types(bytes: &[u8]) -> io::Result<Vec<DataType>> {
    // Example deserialization function
    let mut data_types = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let data_type = match bytes[i] {
            0 => {
                let len = usize::from_be_bytes(bytes[i+1..i+9].try_into().unwrap());
                i += 9; // Advance past the length bytes
                DataType::String(String::from_utf8(bytes[i..i+len].to_vec()).unwrap())
            },
            1 => {
                i += 1;
                DataType::Int(i32::from_be_bytes(bytes[i..i+4].try_into().unwrap()))
            },
            2 => {
                i += 1;
                DataType::Float(f64::from_be_bytes(bytes[i..i+8].try_into().unwrap()))
            },
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown DataType prefix")),
        };
        data_types.push(data_type);
        i += match data_types.last().unwrap() {
            DataType::String(s) => s.len(),
            DataType::Int(_) => 4,
            DataType::Float(_) => 8,
        };
    }
    Ok(data_types)
}