# Databricks notebook source
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), # orphan record, no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]
 
# DataFrame is structured data, schema, column name, column type etc
# column type automatically derived by spark by inspecting data/interference
productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])
brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])
productDf.show()
brandDf.show()

# COMMAND ----------

productDf.createOrReplaceTempView("products")
# products temp table created inside spark session, isolated from other spark session
spark.sql("SELECT * FROM products").show()

# COMMAND ----------

# create new spark session
spark2 = spark.newSession()


# COMMAND ----------

# query products from spark2 will fail , as products inside spark session
spark2.sql("SELECT * FROM PRODUCTS") # error

# COMMAND ----------

# create brands table in global temp view that can be accessible from all spark session within application
brandDf.createOrReplaceGlobalTempView("brands")
# prefix global_temp for global temp view
spark.sql("select * from global_temp.brands").show()

# COMMAND ----------

spark2.sql("select * from global_temp.brands").show()

# COMMAND ----------

