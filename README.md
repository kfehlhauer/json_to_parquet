# json_to_parquet
A Rust implementation of my json2parquet Scala example

Given this JSON:

```[json]
{"VIN": "1A123", "make": "foo", "model": "bar", "year": 2002, "owner": "John Doe", "isRegistered": true} 
{"VIN": "1C123", "make": "foo", "model": "barV2", "year": 2022, "owner": "John Doe Jr.", "isRegistered": false}
{"VIN": "1C123", "make": "foo", "model": "barV2", "year": 2022, "owner": "John Doe Jr."}
```

I want the data stored in the following Parquet schema:
```
message arrow_schema {
  REQUIRED BYTE_ARRAY VIN (STRING);
  REQUIRED BYTE_ARRAY make (STRING);
  REQUIRED BYTE_ARRAY model (STRING);
  REQUIRED INT32 year (INTEGER(16,false));
  REQUIRED BYTE_ARRAY owner (STRING);
  OPTIONAL BOOLEAN isRegistered;
}
```