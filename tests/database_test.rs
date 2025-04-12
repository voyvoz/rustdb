#[cfg(test)]
mod tests {
    use rustdb::database::*;
    use rustdb::interface::*;
    use rustdb::dtype::*;

    #[test]
    fn test_execute_sql() {
        let mut db = Database::new("test_db").unwrap();

        // Create a relation
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("column1".to_string(), vec![
            DataType::Int(1), DataType::Int(42), DataType::Int(3)
        ]);

        relation.columns.insert("column2".to_string(), vec![
            DataType::String("a".to_string()), DataType::String("b".to_string()), DataType::String("c".to_string())
        ]);

        
        // Add relation to the database
        db.add_relation("table".to_string(), relation);

        // Define and execute the SQL query
        let query = "SELECT column1, column2 FROM table WHERE column1 42";
        match db.execute_sql(query) {
            Ok(result) => {
                result.pretty_print();
                // Verify the result
                assert_eq!(result.columns["column1"], vec![DataType::Int(42)]);
                assert_eq!(result.columns["column2"], vec![DataType::String("b".to_string())]);
            },
            Err(e) => panic!("Test failed with error: {}", e),
        }
    }
}
