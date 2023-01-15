use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

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

impl TestMessage {
    /// Utility function to parse a struct of type [TestMessage] into a valid JSON string.
    ///
    /// # Arguments
    ///
    /// * `self` - A reference to the struct of type [TestMessage].
    ///
    /// # Returns
    ///
    /// A JSON string representing the struct of type [TestMessage].
    ///
    /// # Examples
    ///
    /// ~~~text
    ///{"_id":"63c184fcdbc7219b330870d7","crmId":"355e0213-4ce5-4c74-8a8c-e96558f576a1","isActive":true,"age":55,"name":"Hurley Meyer","company":"ONTAGENE","gender":"male","email":"hurleymeyer@ontagene.com"}
    /// ~~~
    pub fn to_json(&self) -> String {
        let json_string = serde_json::to_string(&self).unwrap_or_else(|e| {
            panic!("Failed to parse message to JSON: {}", e);
        });
        json_string
    }
}

impl Display for TestMessage {
    /// Display function for the TestMessage struct. Allows to print out the struct.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Test Message from Kafka with these data: \n\
        ID: {}\n\
        CRM_ID: {}\n\
        Fullname: {}\n\
        Company: {}\n\
        ",
            self._id, self.crmId, self.name, self.company
        )
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
