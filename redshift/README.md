## Redshift Specturum External Schema

```sql
create external schema if not exists <<schema_name>>
from data catalog
database '<<gluecatalogdbname>>'
region 'us-east-2' 
iam_role 'arn:aws:iam::580821237864:role/DELOITTE_REDSHIFT_ROLE';
```
