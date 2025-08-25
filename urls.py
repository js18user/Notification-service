
url_rabbit: str = f"amqp://guest:guest@127.0.0.1/"
url_rabbitmq_aws: str = f"amqps://xxhdmlgy:3VEb46SaBLY5eV7mVP_p0aKj7pMUUcy4@crow.rmq.cloudamqp.com/xxhdmlgy"
url_rabbit_google: str = f"amqps://ilaieekx:2httl0idORgD1d_X4mc_nBYr7Hgxl3TZ@dog.lmq.cloudamqp.com/ilaieekx"

url_keydb: str = f"redis://localhost"
url_redis: str = f"redis://redis-12588.c226.eu-west-1-3.ec2.cloud.redislabs.com:18719"
url_redi: str = f"redis-rediscloud-corrugated:18719"

redis_host: str = f'redis-12588.c226.eu-west-1-3.ec2.cloud.redislabs.com'
redis_port: int = 12588
redis_password: str = f'B7xPvO9hqt6Xpt2jfSxAa2PyWKOPEyW0'

# Postgresql 15  on AWS
db_user_aws: str = f'u5d2osso4ste0g'
db_name_aws: str = f'd3hc7n6us4d3no'
db_password_aws: str = f'p94bca2313004c8089b6cfcd9ec536c2a00e6212b58c63059e26298791dce5972'
db_port_aws: int = 5432
db_host_aws: str = f'cbu5eice9b7g15.cluster-czz5s0kz4scl.eu-west-1.rds.amazonaws.com'

# Postgresql 15  on Local machine
user: str = f'postgres'
name: str = f'fintech'
password: str = f'aa4401'
port: int = 5432
host: str = f'localhost'

query_many: str = f"INSERT INTO message (start_date,status,id_distribution,id_client) VALUES ($1,$2,$3,$4);"

query_ratio: str = (f"SELECT SUM( heap_blks_hit ) / "
                    f"( SUM(heap_blks_hit ) + SUM( heap_blks_read) ) "
                    f"as ratio "
                    f"FROM pg_statio_user_tables;"
)
query_set: str = f"SET max_parallel_workers_per_gather =  4 ;"

# Postgresql 15  on TimeWeb Cloud
url_twp: str = f"postgresql://gen_user:Bv(i%3D%26r5ILjTid@147.45.239.120:5432/default_db"
db_user_twp: str = f"gen_user"
db_name_twp: str = f"default_db"
db_password_twp: str = f"Bv(i%3D%26r5ILjTid"
db_port_twp: int = 5432
db_host_twp: str = f"147.45.239.120"

url_msp: str = 'postgresql://gen_user:Aaa4401&&@192.168.0.5:5432/default_db'

url_azure: str = f"postgresql://neondb_owner:npg_GoPNxHe0pzm4@ep-rapid-glitter-a9y5kqy5-pooler.gwc.azure.neon.tech/neondb?sslmode=require"





