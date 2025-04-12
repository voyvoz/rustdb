#[cfg(test)] 
mod tests {
    use rustdb::interface::*;
    use rustdb::dtype::*;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    #[test]
    fn load_csv() -> std::io::Result<()> {

        let mut tbl = ColumnStoreRelation::new();
        let file_path = "test.csv";
        let tbl_name = "students";

        match tbl.load_csv(file_path, tbl_name, ",", vec!["Number", "Name", "Grade"]) {
            Ok(()) => assert!(true),
            Err(_) => assert!(false),
        }  

        // check name and number of tuples
        let tpls = tbl.num_tuples().expect("Error obtaining number of tuples.");
        let name = tbl.get_table_name().clone();
    
        assert_eq!(tpls, 3);
        assert_eq!(tbl_name, name); 

        // check content of columns
        let grade_rel = tbl.columns.get("Grade").unwrap();
        let name_rel = tbl.columns.get("Name").unwrap();
        let number_rel = tbl.columns.get("Number").unwrap();
        
        let names = ["Name1", "Name2", "Name3"];
        let grades = [1.5, 2.5, 3.33];
        let numbers = [0, 1, 3];

        let mut i = 0;
        for t in name_rel.iter() {
            assert_eq!(DataType::String(names[i].to_string()), t.clone());
            i += 1;
        }

        let mut i = 0;
        for t in grade_rel.iter() {
            assert_eq!(DataType::Float(grades[i]), t.clone());
            i += 1;
        }

        i = 0;
        for t in number_rel.iter() {
            assert_eq!(DataType::Int(numbers[i]), t.clone());
            i += 1;
        }
        Ok(())
    }

    #[test]
    fn test_serialize_deserialize_data_types() {
        // Setup a vector of DataType instances
        let data_types = vec![
            DataType::String("Test".to_string()),
            DataType::Int(42),
            DataType::Float(3.14),
        ];

        // Serialize the DataType instances
        let serialized = serialize_data_types(&data_types).expect("Serialization failed");

        // Deserialize the bytes back into DataType instances
        let deserialized = deserialize_data_types(&serialized).expect("Deserialization failed");

        // Verify that the deserialized data matches the original
        for i in 0..2 {
            assert_eq!(data_types[i], deserialized[i], "Deserialized data does not match original");
        }
    }

    #[test]
    fn select_test() -> std::io::Result<()> {
        let mut tbl = ColumnStoreRelation::new();
        let file_path = "test.csv";
        let tbl_name = "students";

        match tbl.load_csv(file_path, tbl_name, ",", vec!["Number", "Name", "Grade"]) {
            Ok(()) => assert!(true),
            Err(_) => assert!(false),
        }  

        let predicate = |data: &DataType| match data {
            DataType::Float(value) => *value < 5.0,
            _ => false, // Ignore all other data types
        };

        let _selected = tbl.select("Grade", predicate).expect("Error selecting tuples");

        Ok(())
    }

    #[test]
    fn load_csv2() -> std::io::Result<()> {
        let mut tbl = ColumnStoreRelation::new();
        let file_path = "test.csv";
        let tbl_name = "students";

        match tbl.load_csv(file_path, tbl_name, ",", vec!["Number", "Name", "Grade"]) {
            Ok(()) => assert!(true),
            Err(_) => assert!(false),
        }  

        // check name and number of tuples
        let tpls = tbl.num_tuples().expect("Error obtaining number of tuples.");
        let name = tbl.get_table_name().clone();
    
        assert_eq!(tpls, 3);
        assert_eq!(tbl_name, name); 

        // check content of columns
        let grade_rel = tbl.columns.get("Grade").unwrap();
        let name_rel = tbl.columns.get("Name").unwrap();
        let number_rel = tbl.columns.get("Number").unwrap();
        
        let names = ["Name1", "Name2", "Name3"];
        let grades = [1.5, 2.5, 3.33];
        let numbers = [0, 1, 3];

        let mut i = 0;
        for t in name_rel.iter() {
            assert_eq!(DataType::String(names[i].to_string()), t.clone());
            i += 1;
        }

        i = 0;
        for t in grade_rel.iter() {
            assert_eq!(DataType::Float(grades[i]), t.clone());
            i += 1;
        }

        i = 0;
        for t in number_rel.iter() {
            assert_eq!(DataType::Int(numbers[i]), t.clone());
            i += 1;
        }
        Ok(())
    }

    #[test]
    fn test_serialize_deserialize_data_types2() {
        // Setup a vector of DataType instances
        let data_types = vec![
            DataType::String("Test".to_string()),
            DataType::Int(42),
            DataType::Float(3.14),
        ];

        // Serialize the DataType instances
        let serialized = serialize_data_types(&data_types).expect("Serialization failed");

        // Deserialize the bytes back into DataType instances
        let deserialized = deserialize_data_types(&serialized).expect("Deserialization failed");

        // Verify that the deserialized data matches the original
        for i in 0..2 {
            assert_eq!(data_types[i], deserialized[i], "Deserialized data does not match original");
        }
    }

    fn setup_relation() -> ColumnStoreRelation {
        let mut relation = ColumnStoreRelation {
            name: "TestRelation".to_string(),
            fields: HashMap::new(),
            columns: HashMap::new(),
            select_columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            indices: HashMap::new(),
        };

        relation.fields.insert("id".to_string(), DataType::Int(0));
        relation.fields.insert("name".to_string(), DataType::String(String::new()));
        relation.fields.insert("age".to_string(), DataType::Int(0));

        relation.columns.insert("id".to_string(), vec![DataType::Int(1), DataType::Int(2)]);
        relation.columns.insert("name".to_string(), vec![DataType::String("Alice".to_string()), DataType::String("Bob".to_string())]);
        relation.columns.insert("age".to_string(), vec![DataType::Int(30), DataType::Int(25)]);

        relation
    }

    // Test for adding a tuple
    #[test]
    fn test_add_tuple() {
        let mut relation = setup_relation();
        let new_tuple = vec![DataType::Int(3), DataType::String("Charlie".to_string()), DataType::Int(35)];
        assert!(relation.add_tuple(new_tuple).is_ok());
        assert_eq!(relation.columns["id"].len(), 3);
        assert_eq!(relation.columns["name"][2], DataType::String("Charlie".to_string()));
        assert_eq!(relation.columns["age"][2], DataType::Int(35));
    }

    #[test]
    fn test_update_tuple() {
        let mut relation = setup_relation();
        let update_count = relation.update_tuple(
            "age",
            "name",
            |value| matches!(value, DataType::String(name) if name == "Alice"),
            |old_value| {
                if let DataType::Int(age) = old_value {
                    DataType::Int(*age + 5)  // Increasing Alice's age by 5
                } else {
                    old_value.clone()
                }
            }
        ).unwrap();

        assert_eq!(update_count, 1);
        assert_eq!(relation.columns["age"][0], DataType::Int(35));
    }

    #[test]
    fn test_update_tuple2() {
        let mut relation = ColumnStoreRelation::new();

        // Add some columns to the relation
        relation.select_columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        relation.columns.insert("id".to_string(), vec![DataType::Int(1), DataType::Int(2), DataType::Int(3)]);
        relation.columns.insert("name".to_string(), vec![DataType::String("Alice".to_string()), DataType::String("Bob".to_string()), DataType::String("Charlie".to_string())]);
        relation.columns.insert("age".to_string(), vec![DataType::Int(25), DataType::Int(30), DataType::Int(35)]);

        // Define the predicate to select tuples where name is "Bob"
        let predicate = |datum: &DataType| {
            if let DataType::String(name) = datum {
                name == "Bob"
            } else {
                false
            }
        };

        // Define the update function to increase the age by 5
        let update_func = |datum: &DataType| {
            if let DataType::Int(age) = datum {
                DataType::Int(age + 5)
            } else {
                datum.clone()
            }
        };

        // Call the update_tuple method
        let result = relation.update_tuple("age", "name", predicate, update_func);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        // Check the updated values
        let updated_age_column = relation.columns.get("age").unwrap();
        assert_eq!(updated_age_column[0], DataType::Int(25));
        assert_eq!(updated_age_column[1], DataType::Int(35)); // Bob's age should be updated from 30 to 35
        assert_eq!(updated_age_column[2], DataType::Int(35));
    }

    #[test]
    fn test_delete_tuple() {
        let mut relation = setup_relation();
        let delete_count = relation.delete_tuple("name", |value| {
            matches!(value, DataType::String(name) if name == "Bob")
        }).unwrap();

        assert_eq!(delete_count, 1);
        assert_eq!(relation.columns["id"].len(), 1);
        assert_eq!(relation.columns["name"][0], DataType::String("Alice".to_string()));
    }

    #[test]
    fn test_csv_load_save_cycle() {
        // Load data from the temporary file
        let mut relation = ColumnStoreRelation::new();
        relation.load_csv("test.csv", "my_table", ",", vec!["Number", "Name", "Grade"]).unwrap();

        // Save the loaded data to another temporary file
        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_str().unwrap();
        relation.save(output_path).unwrap();

        // Read the saved data from the output file to verify correctness
        let file = std::fs::File::open(output_path).unwrap();
        let mut rdr = csv::Reader::from_reader(file);

        // Collect results from the file
        let headers = rdr.headers().unwrap().clone();
        assert_eq!(headers, vec!["Number", "Name", "Grade"]);

        let mut results = Vec::new();
        for result in rdr.records() {
            let record = result.unwrap();
            results.push(record.iter().map(|s| s.to_string()).collect::<Vec<String>>());
        }

        // Check the contents of the loaded data
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], vec!["0", "Name1", "1.5"]);
        assert_eq!(results[1], vec!["1", "Name2", "2.5"]);
        assert_eq!(results[2], vec!["3", "Name3", "3.33"]);
    }

    
}