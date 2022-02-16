# Databricks notebook source
# copy from iam users..
access_key = ''
secret_key =  ''
encoded_secret_key = secret_key.replace("/", "%2F")
# ****************
aws_bucket_name = ""
# mount name = aws , you can access gks-bucket content as /mnt/aws/movies/movies.csv
mount_name = "aws"

# create a mount point /mnt/aws that points to AWS bucket
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)


display(dbutils.fs.ls("/mnt/%s" % mount_name))



# COMMAND ----------

# don't run this , this will unmount mount point
dbutils.fs.unmount("/mnt/aws")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/%s/raw/ratings" % mount_name))


# COMMAND ----------

