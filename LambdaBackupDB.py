# Matthew Stibbins


import json
import mysql.connector
import boto3
import os

def lambda_handler(event, context):
    # s3 = boto3.client('s3')
    s3 = boto3.resource('s3')  
    for bucket in s3.buckets.all():
        print(bucket.name)


    # Open database connection
    mySQLConnection = mysql.connector.connect(host = os.environ['DB_HOST'],
    database        = os.environ['DB_NAME'],
    user            = os.environ['DB_USER'],
    password        = os.environ['DB_PASS'])

    sql_select_Query = "SELECT `pokemon`.`id`,`pokemon`.`name`,`pokemon`.`typePrimary`,`pokemon`.`typeSecondary`,`pokemon`.`flavorText`FROM `yelpokemon`.`pokemon`"

    # prepare a cursor object using cursor() method
    cursor = mySQLConnection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()

    f = open("/tmp/pokeyelp.data", "w")

    for row in records:
        print(row[0])
        f.write(str(row[0]))
        f.write(" ")
        print(row[1])

        f.write(str(row[1]))
        f.write(" ")
        if(row[2]):
            print(row[2])
            f.write(str(row[2]))
            f.write(" ")
        if(row[3]):
            print(row[3])
            f.write(str(row[3]))
            f.write(" ")
        if(row[4]):
            row[4]
            f.write(str(row[4]))
            f.write(" ")

    f.write("\n")

    f.close()

    data = open('/tmp/pokemon.data', 'rb')
    s3.Bucket('pokeyelp-backupbucket').put_object(Key='pokemon.data', Body=data)


    sql_select_Query = "SELECT `review`.`id`,`review`.`pokemonName`,`review`.`stars`,`review`.`reviewText`FROM `yelpokemon`.`review`;"

    # prepare a cursor object using cursor() method
    cursor = mySQLConnection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()

    f = open("/tmp/reviews.data", "w")

    for row in records:
        print(row[0])
        f.write(str(row[0]))
        f.write(" ")
        print(row[1])

        f.write(str(row[1]))
        f.write(" ")
        if(row[2]):
            print(row[2])
            f.write(str(row[2]))
            f.write(" ")
        if(row[3]):
            print(row[3])
            f.write(str(row[3]))
            f.write(" ")

    f.write("\n")

    f.close()

    data = open('/tmp/pokemon.data', 'rb')
    s3.Bucket('pokeyelp-backupbucket').put_object(Key='reviews.data', Body=data)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


