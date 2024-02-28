connection_oracle_options = {
    "url": "xxxxxx",
    "dbtable": "xxxxxx",
    "user": "xxxxx",
    "password": "xxxxx",
    "sampleQuery" : "SELECT * FROM table"
}
    
dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="oracle",
    connection_options=connection_oracle_options)    
dyf.show()
