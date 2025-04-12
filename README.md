# rustdb
Simple in-memory column store implementation written in Rust

Implemented features:
- In-memory columns and relations
- Import/export of csv files
- Basic SQL operators
	- Scan
	- Select/filter
	- Project
	- Aggregation
	- Joins (Nested loop, sort-merge, hash)
	- Updates
- Basic indexing

Including unit tests and benchmarks. 
