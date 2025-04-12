use crate::dtype::*;
use crate::errors::*;
use crate::interface::*;

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use rayon::prelude::*;
use dashmap::DashMap;

impl ColumnStoreRelation {
    pub fn new() -> ColumnStoreRelation {
        ColumnStoreRelation {
            name: String::new(),
            fields: HashMap::<String, DataType>::new(),
            columns: HashMap::<String, Vec<DataType>>::new(),
            select_columns: Vec::<String>::new(),
            indices: HashMap::<String, BTreeMap<String, Vec<usize>>>::new(),
        }
    }

    pub fn get_columns(&self) -> &HashMap<String, Vec<DataType>> {
        return &self.columns;
    }
}

pub fn calculate_max_width(vec: &Vec<DataType>, column_name: &str) -> Result<usize, &'static str> {
    vec.iter().try_fold(column_name.len(), |max, datum| {
        let formatted_datum = match datum {
            DataType::Float(f) => format!("{:.2}", f),
            _ => format!("{}", datum),
        };
        Ok(std::cmp::max(max, formatted_datum.len()))
    }).or_else(|e : String| Err("Failed to calculate max width"))
}


impl Relation for ColumnStoreRelation {

    fn get_table_name(&self) -> String {
        return self.name.clone();
    }

    fn num_tuples(&self) -> Result<usize, RelationErrors> {
        let size = self.columns.iter()
            .next()  // Retrieves the first (key, value) tuple if there is one
            .map(|(_, v)| v.len())  // Maps the value to its length
            .unwrap_or(0);
        return Ok(size as usize);
    }

    fn load_csv(
        &mut self,
        path: &str,
        table_name: &str, 
        delimiter: &str, 
        select_columns: Vec<&str>,
    ) -> Result<(), RelationErrors> {
        self.columns.clear();
        self.name = table_name.to_string();
        self.select_columns = select_columns.iter().map(|&s| s.to_string()).collect();

        let file = File::open(path)?;
        let mut rdr = csv::Reader::from_reader(file);
        let headers = rdr.headers()?.clone();


        for (_,h) in headers.iter().enumerate() {
            if select_columns.contains(&h) {
                self.columns.insert(h.to_string(), Vec::<DataType>::new());
            }
        }

        for result in rdr.records() {
            let record = result?;
            for (index, field) in record.iter().enumerate() {
                if let Some(column_name) = headers.get(index) {
                    if let Some(column) = self.columns.get_mut(column_name) {
                        column.push(DataType::from_str(field)); 
                    }
                }
            }
        }

        Ok(())
    }

    fn save(&self, path: &str) -> Result<(), RelationErrors> {
        let file = File::create(path).map_err(|e| {
            RelationErrors::WriteError(e.to_string())
        })?;

        let mut writer = csv::Writer::from_writer(file);

        if !self.select_columns.is_empty() {
            writer.write_record(&self.select_columns).map_err(|e| {
                RelationErrors::WriteError(e.to_string())
            })?;
        }

        let max_rows = self.columns.values().map(|col| col.len()).max().unwrap_or(0);

        for row_index in 0..max_rows {
            let mut row: Vec<String> = Vec::new();

            for column_name in &self.select_columns {
                let value = if let Some(column) = self.columns.get(column_name) {
                    column.get(row_index).map_or(String::new(), |v| v.to_string())
                } else {
                    String::new()
                };
                row.push(value);
            }

            writer.write_record(&row).map_err(|e| {
                RelationErrors::WriteError(e.to_string())
            })?;
        }

        writer.flush().map_err(|e| {
            RelationErrors::WriteError(e.to_string())
        })?;

        Ok(())
    }
    
    fn get_select_columns(&self) -> &Vec<String> {
        return &self.select_columns;
    }

    fn pretty_print(&self) {
        let column_widths = DashMap::new();

        // Calculate column widths based on `select_columns` to maintain order
        self.select_columns.par_iter().for_each(|column_name| {
            if let Some(data) = self.columns.get(column_name) {
                if let Some(max_width) = calculate_max_width(data, column_name).ok() {
                    column_widths.insert(column_name.clone(), max_width);
                }
            }
        });

        // Create horizontal separator lines based on individual column widths
        let horizontal_line: String = self.select_columns.iter()
            .filter_map(|name| column_widths.get(name).map(|entry| *entry.value()))
            .map(|width| "─".repeat(width + 2))
            .collect::<Vec<String>>()
            .join("┼");

        let top_border = format!("┌{}┐", horizontal_line.replace('┼', "┬"));
        let header_separator = format!("├{}┤", horizontal_line);
        let bottom_border = format!("└{}┘", horizontal_line.replace('┼', "┴"));

        // Estimate the total capacity needed for the output string
        let max_rows = self.columns.values().map(|col| col.len()).max().unwrap_or(0);
        let estimated_capacity = top_border.len() + header_separator.len() + bottom_border.len()
            + (self.select_columns.iter().map(|col| column_widths.get(col).map_or(0, |entry| *entry.value() + 4)).sum::<usize>() + 3) * (max_rows + 2);

        let mut output = String::with_capacity(estimated_capacity);

        // Collect the top border
        output.push_str(&top_border);
        output.push('\n');

        // Collect Headers with padding, in the order specified by `select_columns`
        output.push('│');
        for column_name in &self.select_columns {
            if let Some(width) = column_widths.get(column_name).map(|entry| *entry.value()) {
                output.push_str(&format!(" {:width$} │", column_name, width = width));
            }
        }
        output.push('\n');

        // Separator after header
        output.push_str(&header_separator);
        output.push('\n');

        // Collect Data Rows with padding, following the order of `select_columns`
        let rows_output: Vec<String> = (0..max_rows).into_par_iter().map(|row| {
            let mut row_output = String::with_capacity(self.select_columns.len() * 10); // Adjust the capacity as needed
            row_output.push('│');
            for column_name in &self.select_columns {
                if let Some(data) = self.columns.get(column_name) {
                    let value = data.get(row).map_or(String::new(), |v| match *v {
                        DataType::Float(f) => format!("{:.2}", f),
                        _ => format!("{}", v),
                    });
                    if let Some(width) = column_widths.get(column_name).map(|entry| *entry.value()) {
                        row_output.push_str(&format!(" {:width$} │", value, width = width));
                    }
                }
            }
            row_output.push('\n');
            row_output
        }).collect();

        for row in rows_output {
            output.push_str(&row);
        }

        // Collect the bottom border
        output.push_str(&bottom_border);
        output.push('\n');

        // Print the collected output
        println!("{}", output);
    }

    fn select<F>(&mut self, column_name: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where
        F: Fn(&DataType) -> bool,
    {
        // First, check if the column exists
        let column_data = self.columns.get(column_name)
            .ok_or(RelationErrors::ColumnNotFound(column_name.to_string()))?;

        // Collect indices of rows where the predicate is true
        let matching_indices: Vec<usize> = column_data.iter()
            .enumerate()
            .filter_map(|(index, datum)| {
                if predicate(datum) {
                    Some(index)
                } else {
                    None
                }
            })
            .collect();

        // Create a new IntermediateRelation to hold the filtered results
        let mut result_relation = ColumnStoreRelation::new();
        result_relation.name = self.name.clone(); // Assuming you want to keep the same relation name

        // Filter all columns based on matching indices
        for (key, values) in &self.columns {
            let filtered_values: Vec<DataType> = matching_indices.iter()
                .filter_map(|&index| values.get(index).cloned())
                .collect();
            result_relation.columns.insert(key.clone(), filtered_values);
        }

        // Optionally copy the indices, if they need to be filtered similarly
        for (key, index_map) in &self.indices {
            let filtered_index_map: BTreeMap<String, Vec<usize>> = index_map.iter()
                .filter_map(|(value_key, rows)| {
                    let filtered_rows: Vec<usize> = rows.iter()
                        .filter(|row_index| matching_indices.contains(row_index))
                        .cloned()
                        .collect();
                    if !filtered_rows.is_empty() {
                        Some((value_key.clone(), filtered_rows))
                    } else {
                        None
                    }
                })
                .collect();
            result_relation.indices.insert(key.clone(), filtered_index_map);
        }

        // Maintain selected column order and fields in the new relation
        result_relation.select_columns = self.select_columns.clone();
        result_relation.fields = self.fields.clone();

        Ok(result_relation)
    }

    fn project(&self, columns_to_keep: Vec<&str>) -> Result<ColumnStoreRelation, RelationErrors> {
        // Create a new ColumnStoreRelation to hold the result
        let mut result_relation = ColumnStoreRelation::new();
        result_relation.name = self.name.clone(); // Keep the same name

        // Iterate over the specified columns, adding them to the new relation if they exist
        for column_name in columns_to_keep {
            if let Some(data) = self.columns.get(column_name) {
                // If the column exists, copy it to the new relation
                result_relation.columns.insert(column_name.to_string(), data.clone());
                // Also, add to select_columns to maintain the order
                result_relation.select_columns.push(column_name.to_string());
            } else {
                // If a specified column does not exist, you might want to return an error
                return Err(RelationErrors::ColumnNotFound(column_name.to_string()));
                // Or simply skip this column
                // continue;
            }
        }

        Ok(result_relation)
    }

    fn aggr(&self, column_name: &str, aggregation: Aggregation) -> Result<DataType, RelationErrors> {
        match self.columns.get(column_name) {
            Some(column) => {
                match aggregation {
                    Aggregation::Count => Ok(DataType::Int(column.len() as i32)),
                    Aggregation::Sum => {
                        let sum = column.iter().try_fold(0f64, |acc, val| {
                            if let DataType::Int(i) = val {
                                Ok(acc + (*i as f64))
                            } else if let DataType::Float(f) = val {
                                Ok(acc + f)
                            } else {
                                Err(RelationErrors::Error("Sum operation on non-numeric column".to_string()))
                            }
                        })?;
                        Ok(DataType::Float(sum))
                    },
                    Aggregation::Min => {
                        let min = column.iter().filter_map(|val| match val {
                            DataType::Int(i) => Some(*i as f64),
                            DataType::Float(f) => Some(*f),
                            _ => None,
                        }).fold(f64::INFINITY, |a, b| a.min(b));
                    
                        if min == f64::INFINITY {
                            Err(RelationErrors::Error("Min operation on non-numeric column or empty column".to_string()))
                        } else {
                            Ok(DataType::Float(min))
                        }
                    },
                    Aggregation::Max => {
                        let max = column.iter().filter_map(|val| match val {
                            DataType::Int(i) => Some(*i as f64),
                            DataType::Float(f) => Some(*f),
                            _ => None,
                        }).fold(f64::NEG_INFINITY, |a, b| a.max(b));
                    
                        if max == f64::NEG_INFINITY {
                            Err(RelationErrors::Error("Max operation on non-numeric column or empty column".to_string()))
                        } else {
                            Ok(DataType::Float(max))
                        }
                    },
                    Aggregation::Average => {
                        let sum = column.iter().filter_map(|val| match val {
                            DataType::Int(i) => Some(*i as f64),
                            DataType::Float(f) => Some(*f),
                            _ => None,
                        }).sum::<f64>();
                        let count = column.iter().filter_map(|val| match val {
                            DataType::Int(_) | DataType::Float(_) => Some(1),
                            _ => None,
                        }).count();
                        
                        if count > 0 {
                            Ok(DataType::Float(sum / count as f64))
                        } else {
                            Err(RelationErrors::Error("Average operation on non-numeric column or empty column".to_string()))
                        }
                    },
                }
            },
            None => Err(RelationErrors::ColumnNotFound(column_name.to_string())),
        }
    }

    fn sort(&mut self, column_name: &str, order: Order) -> Result<(), RelationErrors> {
        let sort_column = self.columns.get(column_name)
            .ok_or(RelationErrors::ColumnNotFound(column_name.to_string()))?;

        let mut indices: Vec<usize> = (0..sort_column.len()).collect();
        
        indices.sort_by(|&a, &b| {
            let val_a = &sort_column[a];
            let val_b = &sort_column[b];
            let cmp = match (val_a, val_b) {
                (DataType::Int(int_a), DataType::Int(int_b)) => int_a.cmp(int_b),
                (DataType::Float(float_a), DataType::Float(float_b)) => float_a.partial_cmp(float_b).unwrap_or(std::cmp::Ordering::Equal),
                (DataType::String(str_a), DataType::String(str_b)) => str_a.cmp(str_b),
                
                _ => std::cmp::Ordering::Equal, 
            };

            match order {
                Order::Asc => cmp,
                Order::Desc => cmp.reverse(),
            }
        });

        for column in self.columns.values_mut() {
            let sorted_column: Vec<DataType> = indices.iter().map(|&i| column[i].clone()).collect();
            *column = sorted_column;
        }

        Ok(())
    }

    fn create_index(&mut self, column_name: &str) -> Result<(), String> {
        if !self.columns.contains_key(column_name) {
            return Err("Column not found".to_string());
        }

        let column_data = self.columns.get(column_name).unwrap();
        let mut index = BTreeMap::new();

        for (row_idx, value) in column_data.iter().enumerate() {
            let key = value.to_str();
            index.entry(key).or_insert_with(Vec::new).push(row_idx);
        }

        self.indices.insert(column_name.to_string(), index);

        Ok(())
    }

    fn index_select<F>(&self, column_name: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where
        F: Fn(&DataType) -> bool,
    {
        if let Some(index) = self.indices.get(column_name) {
            let mut result_relation = ColumnStoreRelation::new();
            result_relation.name = self.name.clone();
            result_relation.fields = self.fields.clone(); 

            let mut matched_indices: Vec<usize> = Vec::new();
            for (value, row_indices) in index {
                if predicate(&DataType::from_str(value)) {
                    matched_indices.extend(row_indices);
                }
            }

            matched_indices.sort();
            matched_indices.dedup();

            for (key, value) in self.columns.iter() {
                let filtered_column_data: Vec<DataType> = matched_indices.iter()
                    .filter_map(|&i| value.get(i).cloned())
                    .collect();
                result_relation.columns.insert(key.clone(), filtered_column_data);
            }

            result_relation.select_columns = self.select_columns.clone();
            Ok(result_relation)
        } else {
            Err(RelationErrors::ColumnNotFound(column_name.to_string()))
        }
    }

    fn scan<F>(&mut self, select_columns: Vec<&str>, predicate: F) -> Result<ColumnStoreRelation, RelationErrors> 
    where F: Fn(&DataType) -> bool 
    {
        let mut new_relation = ColumnStoreRelation::new();
        new_relation.name = self.name.clone();

        for column_name in select_columns.iter().map(|s| s.to_string()) {
            if let Some(column_data) = self.columns.get(&column_name) {
                let mut filtered_data = Vec::new();
                
                for datum_result in column_data.iter().map(|datum| datum) { 
                    match datum_result {
                        datum => {
                            if predicate(datum) {
                                filtered_data.push(datum.clone());
                            }
                        },
                        err => {
                            continue;
                        }
                    }
                }

                // Only insert non-empty data sets into the new relation
                if !filtered_data.is_empty() {
                    new_relation.columns.insert(column_name.clone(), filtered_data);
                }
            } else {
                return Err(RelationErrors::ColumnNotFound(column_name));
            }
        }

        new_relation.select_columns = select_columns.iter().map(|s| s.to_string()).collect();

        Ok(new_relation)
    }

    fn nested_loop_join<F>(&self, other_relation: &ColumnStoreRelation, r_col: &str, s_col: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where F: Fn(&DataType, &DataType) -> bool 
    {
        // Ensure both columns exist in their respective relations
        let r_col_data = self.columns.get(r_col)
            .ok_or_else(|| RelationErrors::ColumnNotFound(r_col.to_string()))?;
        let s_col_data = other_relation.columns.get(s_col)
            .ok_or_else(|| RelationErrors::ColumnNotFound(s_col.to_string()))?;

        // Create a new relation to store the join result
        let mut result_relation = ColumnStoreRelation::new();
        result_relation.name = format!("{}_{}_join", self.name, other_relation.name);

        // Copy the field definitions and selected columns from both relations, avoiding duplicate columns
        for (key, value) in &self.fields {
            result_relation.fields.insert(key.clone(), value.clone());
        }
        for (key, value) in &other_relation.fields {
            if key != s_col {
                result_relation.fields.insert(key.clone(), value.clone());
            }
        }

        result_relation.select_columns = self.select_columns.iter()
            .chain(other_relation.select_columns.iter().filter(|&col| col != s_col))
            .cloned()
            .collect();

        for column_name in &result_relation.select_columns {
            result_relation.columns.insert(column_name.clone(), Vec::new());
        }

        // Perform nested loop join
        for (i, r_value) in r_col_data.iter().enumerate() {
            for (j, s_value) in s_col_data.iter().enumerate() {
                if predicate(r_value, s_value) {
                    // Add the values from the first relation
                    for (key, values) in &self.columns {
                        if let Some(column) = result_relation.columns.get_mut(key) {
                            column.push(values[i].clone());
                        }
                    }
                    // Add the values from the second relation, except for the join column
                    for (key, values) in &other_relation.columns {
                        if key != s_col {
                            if let Some(column) = result_relation.columns.get_mut(key) {
                                column.push(values[j].clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(result_relation)
    }

    fn merge_join<F>(&self, other_relation: &ColumnStoreRelation, r_col: &str, s_col: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where F: Fn(&DataType, &DataType) -> bool 
    {
        let r_col_data = self.columns.get(r_col)
            .ok_or_else(|| RelationErrors::ColumnNotFound(r_col.to_string()))?;
        let s_col_data = other_relation.columns.get(s_col)
            .ok_or_else(|| RelationErrors::ColumnNotFound(s_col.to_string()))?;

        // Check if both columns are sorted
        let mut r_sorted = true;
        for i in 1..r_col_data.len() {
            if r_col_data[i] < r_col_data[i-1] {
                r_sorted = false;
                break;
            }
        }
        
        let mut s_sorted = true;
        for i in 1..s_col_data.len() {
            if s_col_data[i] < s_col_data[i-1] {
                s_sorted = false;
                break;
            }
        }
        
        if !r_sorted || !s_sorted {
            return Err(RelationErrors::Error("Columns are not sorted for merge join".to_string()));
        }

        // Create a new relation to store the join result
        let mut result_relation = ColumnStoreRelation::new();
        result_relation.name = format!("{}_{}_join", self.name, other_relation.name);

        // Copy the field definitions and selected columns from both relations, avoiding duplicate columns
        for (key, value) in &self.fields {
            result_relation.fields.insert(key.clone(), value.clone());
        }
        for (key, value) in &other_relation.fields {
            if key != s_col {
                result_relation.fields.insert(key.clone(), value.clone());
            }
        }

        // Combine the selected columns without duplicating the join column
        result_relation.select_columns = self.select_columns.iter()
            .chain(other_relation.select_columns.iter().filter(|&col| col != s_col))
            .cloned()
            .collect();

        // Initialize result columns
        for column_name in &result_relation.select_columns {
            result_relation.columns.insert(column_name.clone(), Vec::new());
        }

        // Perform merge join
        let mut i = 0;
        let mut j = 0;

        while i < r_col_data.len() && j < s_col_data.len() {
            if predicate(&r_col_data[i], &s_col_data[j]) {
                let mut k = j;
                while k < s_col_data.len() && s_col_data[k] == s_col_data[j] {
                    // Combine the tuples from both relations
                    for (key, values) in &self.columns {
                        if let Some(column) = result_relation.columns.get_mut(key) {
                            column.push(values[i].clone());
                        }
                    }
                    for (key, values) in &other_relation.columns {
                        if key != s_col {
                            if let Some(column) = result_relation.columns.get_mut(key) {
                                column.push(values[k].clone());
                            }
                        }
                    }
                    k += 1;
                }
                i += 1;
            } else if r_col_data[i] < s_col_data[j] {
                i += 1;
            } else {
                j += 1;
            }
        }

        Ok(result_relation)
    }

    fn hash_join<F>(&self, other_relation: &ColumnStoreRelation, r_col: &str, s_col: &str, predicate: F) -> Result<ColumnStoreRelation, RelationErrors>
    where F: Fn(&DataType, &DataType) -> bool 
    {
        // Ensure both columns exist in their respective relations
        if !self.columns.contains_key(r_col) {
            return Err(RelationErrors::ColumnNotFound(r_col.to_string()));
        }
        if !other_relation.columns.contains_key(s_col) {
            return Err(RelationErrors::ColumnNotFound(s_col.to_string()));
        }

        // Create a new relation to store the join result
        let mut result_relation = ColumnStoreRelation::new();
        result_relation.name = format!("{}_{}_join", self.name, other_relation.name);

        // Copy the field definitions and selected columns from both relations, avoiding duplicate columns
        for (key, value) in &self.fields {
            result_relation.fields.insert(key.clone(), value.clone());
        }
        for (key, value) in &other_relation.fields {
            if key != s_col {
                result_relation.fields.insert(key.clone(), value.clone());
            }
        }

        // Combine the selected columns without duplicating the join column
        result_relation.select_columns = self.select_columns.iter()
            .chain(other_relation.select_columns.iter().filter(|&col| col != s_col))
            .cloned()
            .collect();

        // Initialize result columns
        for column_name in &result_relation.select_columns {
            result_relation.columns.insert(column_name.clone(), Vec::new());
        }

        // Build the hash table for the first relation
        let mut hash_table: HashMap<&DataType, Vec<usize>> = HashMap::new();
        let r_col_data = self.columns.get(r_col).unwrap();
        for (i, value) in r_col_data.iter().enumerate() {
            hash_table.entry(value).or_insert_with(Vec::new).push(i);
        }

        // Probe the hash table with the second relation
        let s_col_data = other_relation.columns.get(s_col).unwrap();
        for (j, s_value) in s_col_data.iter().enumerate() {
            if let Some(indices) = hash_table.get(s_value) {
                for &i in indices {
                    if predicate(&r_col_data[i], s_value) {
                        // Add the values from the first relation
                        for (key, values) in &self.columns {
                            if let Some(column) = result_relation.columns.get_mut(key) {
                                column.push(values[i].clone());
                            }
                        }
                        // Add the values from the second relation, except for the join column
                        for (key, values) in &other_relation.columns {
                            if key != s_col {
                                if let Some(column) = result_relation.columns.get_mut(key) {
                                    column.push(values[j].clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result_relation)
    }

    fn add_tuple(&mut self, tuple: Vec<DataType>) -> Result<(), RelationErrors> {
        // Check if the tuple has the correct number of elements
        if tuple.len() != self.select_columns.len() {
            return Err(RelationErrors::InvalidInput("Tuple does not match relation schema".to_string()));
        }

        // Add each element of the tuple to the corresponding column
        for (index, data) in tuple.into_iter().enumerate() {
            let column_name = &self.select_columns[index]; // get the column name from select_columns based on index

            // Check if the given column name exists in the columns map
            if !self.columns.contains_key(column_name) {
                return Err(RelationErrors::ColumnNotFound(column_name.to_string()));
            }

            // Append the data to the corresponding column
            self.columns.get_mut(column_name).unwrap().push(data);
        }

        Ok(())
    }

    fn delete_tuple<F>(&mut self, column_name: &str, predicate: F) -> Result<usize, RelationErrors>
    where
        F: Fn(&DataType) -> bool,
    {
        // Check if the specified column exists
        if !self.columns.contains_key(column_name) {
            return Err(RelationErrors::ColumnNotFound(column_name.to_string()));
        }

        // Find indices of all rows to be deleted
        let rows_to_delete: Vec<usize> = self.columns[column_name]
            .iter()
            .enumerate()
            .filter_map(|(index, value)| if predicate(value) { Some(index) } else { None })
            .collect();

        // If no rows to delete, return early
        if rows_to_delete.is_empty() {
            return Ok(0);
        }

        // Delete elements in all columns based on the indices found
        for column_data in self.columns.values_mut() {
            let mut i = 0;
            column_data.retain(|_| {
                let retain = !rows_to_delete.contains(&(i));
                i += 1;
                retain
            });
        }

        Ok(rows_to_delete.len())
    }

    fn update_tuple<F, G>(&mut self, target_column: &str, filter_column: &str, predicate: F, update_func: G) -> Result<usize, RelationErrors>
    where
        F: Fn(&DataType) -> bool, // Predicate function to select tuples to update
        G: Fn(&DataType) -> DataType, // Function to update the selected tuples
    {
        // Check if the target and filter columns exist
        if !self.columns.contains_key(target_column) || !self.columns.contains_key(filter_column) {
            return Err(RelationErrors::ColumnNotFound(format!("{} or {} not found", target_column, filter_column)));
        }

        // Get references to the filter and target columns
        let filter_column_data = self.columns[filter_column].clone();
        let target_column_data = self.columns.get_mut(target_column).unwrap();

        // Iterate over the filter column to find indices of rows to update
        let mut updated_count = 0;
        for (index, value) in filter_column_data.iter().enumerate() {
            if predicate(value) {
                // Update the corresponding value in the target column
                target_column_data[index] = update_func(&target_column_data[index]);
                updated_count += 1;
            }
        }

        Ok(updated_count)
    }


}
