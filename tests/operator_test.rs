#[cfg(test)] 
mod tests {
    use rustdb::interface::*;
    use rustdb::dtype::*;
    
    fn generate_random_data() -> (Vec<DataType>, Vec<DataType>, Vec<DataType>) {
        let ids: Vec<DataType> = (1..=100).map(DataType::Int).collect();
        
        let names: Vec<DataType> = (0..100).map(|i| {
            let name = format!("name{}", i);
            DataType::String(name)
        }).collect();
        
        let ages: Vec<DataType> = (0..100).map(|i| DataType::Int(18 + (i % 52))).collect();
        
        (ids, names, ages)
    }

    #[test]
    fn test_select() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("id".to_string(), vec![
            DataType::Int(1), DataType::Int(2), DataType::Int(3), DataType::Int(4)
        ]);
        relation.columns.insert("name".to_string(), vec![
            DataType::String("Alice".to_string()), DataType::String("Bob".to_string()), DataType::String("Charlie".to_string()), DataType::String("David".to_string())
        ]);
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ]);

        // Define the predicate function
        let predicate = |datum: &DataType| {
            if let DataType::Int(age) = datum {
                *age > 30
            } else {
                false
            }
        };

        // Perform the selection
        let result = relation.select("age", predicate);

        // Ensure the result is Ok
        assert!(result.is_ok());

        // Unwrap the result to get the new relation
        let selected_relation = result.unwrap();

        // Validate the selected data
        assert_eq!(selected_relation.get_columns().get("id").unwrap(), &vec![
            DataType::Int(3), DataType::Int(4)
        ]);
        assert_eq!(selected_relation.get_columns().get("name").unwrap(), &vec![
            DataType::String("Charlie".to_string()), DataType::String("David".to_string())
        ]);
        assert_eq!(selected_relation.get_columns().get("age").unwrap(), &vec![
            DataType::Int(35), DataType::Int(40)
        ]);
    }

    #[test]
    fn test_select_with_random_data() {
        // Initialize the ColumnStoreRelation and load some random data
        let (ids, names, ages) = generate_random_data();
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("id".to_string(), ids);
        relation.columns.insert("name".to_string(), names);
        relation.columns.insert("age".to_string(), ages);

        // Define the predicate function
        let predicate = |datum: &DataType| {
            if let DataType::Int(age) = datum {
                *age > 30
            } else {
                false
            }
        };

        // Perform the selection
        let result = relation.select("age", predicate);

        // Ensure the result is Ok
        assert!(result.is_ok());

        // Unwrap the result to get the new relation
        let selected_relation = result.unwrap();

        // Validate the selected data
        let selected_ages = selected_relation.get_columns().get("age").unwrap();
        for age in selected_ages {
            if let DataType::Int(age_value) = age {
                assert!(*age_value > 30);
            } else {
                panic!("Non-integer value found in age column");
            }
        }

        // Ensure that the selected rows have consistent lengths across columns
        let selected_ids = selected_relation.get_columns().get("id").unwrap();
        let selected_names = selected_relation.get_columns().get("name").unwrap();
        assert_eq!(selected_ids.len(), selected_names.len());
        assert_eq!(selected_ids.len(), selected_ages.len());
    }

    #[test]
    fn test_project() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("id".to_string(), vec![
            DataType::Int(1), DataType::Int(2), DataType::Int(3), DataType::Int(4)
        ]);
        relation.columns.insert("name".to_string(), vec![
            DataType::String("Alice".to_string()), DataType::String("Bob".to_string()), DataType::String("Charlie".to_string()), DataType::String("David".to_string())
        ]);
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ]);
        relation.select_columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];

        // Define the columns to project
        let columns_to_keep = vec!["id", "name"];

        // Perform the projection
        let result = relation.project(columns_to_keep);

        // Ensure the result is Ok
        assert!(result.is_ok());

        // Unwrap the result to get the new relation
        let projected_relation = result.unwrap();

        // Validate the projected data
        let expected_ids = vec![
            DataType::Int(1), DataType::Int(2), DataType::Int(3), DataType::Int(4)
        ];
        let expected_names = vec![
            DataType::String("Alice".to_string()), DataType::String("Bob".to_string()), DataType::String("Charlie".to_string()), DataType::String("David".to_string())
        ];

        assert_eq!(projected_relation.get_columns().get("id").unwrap(), &expected_ids);
        assert_eq!(projected_relation.get_columns().get("name").unwrap(), &expected_names);
        assert!(projected_relation.get_columns().get("age").is_none());

        // Ensure that the projected relation's select_columns contains only the projected columns
        assert_eq!(projected_relation.get_select_columns(), &vec!["id".to_string(), "name".to_string()]);
    }
    
    #[test]
    fn test_sort_ascending() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("id".to_string(), vec![
            DataType::Int(4), DataType::Int(2), DataType::Int(3), DataType::Int(1)
        ]);
        relation.columns.insert("name".to_string(), vec![
            DataType::String("David".to_string()), DataType::String("Bob".to_string()), DataType::String("Charlie".to_string()), DataType::String("Alice".to_string())
        ]);
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(40), DataType::Int(25), DataType::Int(35), DataType::Int(30)
        ]);
        relation.select_columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];

        // Sort by the "id" column in ascending order
        let result = relation.sort("id", Order::Asc);

        // Ensure the result is Ok
        assert!(result.is_ok());

        // Validate the sorted data
        let expected_ids = vec![
            DataType::Int(1), DataType::Int(2), DataType::Int(3), DataType::Int(4)
        ];
        let expected_names = vec![
            DataType::String("Alice".to_string()), DataType::String("Bob".to_string()), DataType::String("Charlie".to_string()), DataType::String("David".to_string())
        ];
        let expected_ages = vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ];

        assert_eq!(relation.get_columns().get("id").unwrap(), &expected_ids);
        assert_eq!(relation.get_columns().get("name").unwrap(), &expected_names);
        assert_eq!(relation.get_columns().get("age").unwrap(), &expected_ages);
    }

    #[test]
    fn test_sort_descending() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("id".to_string(), vec![
            DataType::Int(4), DataType::Int(2), DataType::Int(3), DataType::Int(1)
        ]);
        relation.columns.insert("name".to_string(), vec![
            DataType::String("David".to_string()), DataType::String("Bob".to_string()), DataType::String("Charlie".to_string()), DataType::String("Alice".to_string())
        ]);
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(40), DataType::Int(25), DataType::Int(35), DataType::Int(30)
        ]);
        relation.select_columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];

        // Sort by the "id" column in descending order
        let result = relation.sort("id", Order::Desc);

        // Ensure the result is Ok
        assert!(result.is_ok());

        // Validate the sorted data
        let expected_ids = vec![
            DataType::Int(4), DataType::Int(3), DataType::Int(2), DataType::Int(1)
        ];
        let expected_names = vec![
            DataType::String("David".to_string()), DataType::String("Charlie".to_string()), DataType::String("Bob".to_string()), DataType::String("Alice".to_string())
        ];
        let expected_ages = vec![
            DataType::Int(40), DataType::Int(35), DataType::Int(25), DataType::Int(30)
        ];

        assert_eq!(relation.get_columns().get("id").unwrap(), &expected_ids);
        assert_eq!(relation.get_columns().get("name").unwrap(), &expected_names);
        assert_eq!(relation.get_columns().get("age").unwrap(), &expected_ages);
    }

    #[test]
    fn test_aggr_count() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ]);

        // Perform the count aggregation
        let result = relation.aggr("age", Aggregation::Count);

        // Ensure the result is Ok and correct
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Int(4));
    }

    #[test]
    fn test_aggr_sum() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ]);

        // Perform the sum aggregation
        let result = relation.aggr("age", Aggregation::Sum);

        // Ensure the result is Ok and correct
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Float(130.0)); // Sum: 30 + 25 + 35 + 40 = 130
    }

    #[test]
    fn test_aggr_min() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ]);

        // Perform the min aggregation
        let result = relation.aggr("age", Aggregation::Min);

        // Ensure the result is Ok and correct
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Float(25.0)); // Min: 25
    }

    #[test]
    fn test_aggr_max() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ]);

        // Perform the max aggregation
        let result = relation.aggr("age", Aggregation::Max);

        // Ensure the result is Ok and correct
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Float(40.0)); // Max: 40
    }

    #[test]
    fn test_aggr_average() {
        // Initialize the ColumnStoreRelation and load some data
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("age".to_string(), vec![
            DataType::Int(30), DataType::Int(25), DataType::Int(35), DataType::Int(40)
        ]);

        // Perform the average aggregation
        let result = relation.aggr("age", Aggregation::Average);

        // Ensure the result is Ok and correct
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Float(32.5)); // Average: (30 + 25 + 35 + 40) / 4 = 32.5
    }

    #[test]
    fn test_create_index() {
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("column1".to_string(), vec![
            DataType::Int(1),
            DataType::Int(2),
            DataType::Int(2),
            DataType::Int(3)
        ]);
        relation.columns.insert("column2".to_string(), vec![
            DataType::String("a".to_string()),
            DataType::String("b".to_string()),
            DataType::String("b".to_string()),
            DataType::String("c".to_string())
        ]);
        relation.select_columns = vec!["column1".to_string(), "column2".to_string()];

        // Create an index on column1
        relation.create_index("column1").expect("Failed to create index on column1");

        // Verify the index
        let index = relation.indices.get("column1").expect("Index not found");
        assert_eq!(index.get("1").unwrap(), &vec![0]);
        assert_eq!(index.get("2").unwrap(), &vec![1, 2]);
        assert_eq!(index.get("3").unwrap(), &vec![3]);
    }

    #[test]
    fn test_index_select() {
        let mut relation = ColumnStoreRelation::new();
        relation.columns.insert("column1".to_string(), vec![
            DataType::Int(1),
            DataType::Int(2),
            DataType::Int(2),
            DataType::Int(3)
        ]);
        relation.columns.insert("column2".to_string(), vec![
            DataType::String("a".to_string()),
            DataType::String("b".to_string()),
            DataType::String("b".to_string()),
            DataType::String("c".to_string())
        ]);
        relation.select_columns = vec!["column1".to_string(), "column2".to_string()];

        // Create an index on column1
        relation.create_index("column1").expect("Failed to create index on column1");

        // Perform index-based selection
        let result = relation.index_select("column1", |d| {
            if let DataType::Int(val) = d {
                *val == 2
            } else {
                false
            }
        }).expect("Failed to select using index");

        // Verify the result
        assert_eq!(result.columns["column1"], vec![DataType::Int(2), DataType::Int(2)]);
        assert_eq!(result.columns["column2"], vec![DataType::String("b".to_string()), DataType::String("b".to_string())]);
    }

}