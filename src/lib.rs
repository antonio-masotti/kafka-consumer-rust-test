use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};

/// Data model for the messages received or sent to Kafka
#[derive(Serialize, Deserialize)]
pub struct TestMessage {
    pub _id: String,
    pub crmId: String,
    pub isActive: bool,
    pub age: i32,
    pub name: String,
    pub company: String,
    pub gender: String,
    pub email: String,
}

impl Display for TestMessage {
    /// Display function for the TestMessage struct. Allows to print out the struct.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Test Message from Kafka with these data: \n\
        ID: {}\n\
        CRM_ID: {}\n\
        Fullname: {}\n
        Company: {}\n\
        ", self._id, self.crmId, self.name, self.company)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_message_serialization() {
        let json_representation = r#"{
            "_id":"5f7b5b9b0b5b9b0b5b9b0b5b",
            "crmId":"5f7b5b9b0b5b9b0b5b9b0b5b",
            "isActive":true,
            "age":32,
            "gender": "male",
            "name": "Mccarty Mccarty",
            "company":"ZILLACOM",
            "email": "test@gmail.com"
            }"#;

        let test_message: TestMessage = serde_json::from_str(json_representation).unwrap();
        assert_eq!(test_message._id, "5f7b5b9b0b5b9b0b5b9b0b5b");
        assert_eq!(test_message.crmId, "5f7b5b9b0b5b9b0b5b9b0b5b");
        assert_eq!(test_message.isActive, true);
        assert_eq!(test_message.age, 32);
        assert_eq!(test_message.gender, "male");
        assert_eq!(test_message.name, "Mccarty Mccarty");
        assert_eq!(test_message.company, "ZILLACOM");
        assert_eq!(test_message.email, "test@gmail.com");
    }
}