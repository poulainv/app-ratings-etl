resource "aws_glue_catalog_database" "database" {
  name = "blinkist"
}

resource "aws_glue_catalog_table" "ratings" {
  name          = "ratings"
  database_name = aws_glue_catalog_database.database.name

  table_type = "EXTERNAL_TABLE"

  storage_descriptor {
  
    location = "s3://${aws_s3_bucket.bucket.bucket}/ratings"

    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ratings"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1
      }
    }

    columns {
      name = "published_date"
      type = "string"
    }

    columns {
      name = "rating"
      type = "int"
    }

    columns {
      name = "author"
      type = "string"
    }
    columns {
      name = "platform"
      type = "string"
    }
  }
}