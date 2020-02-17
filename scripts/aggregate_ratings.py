import boto3

AGGREGATE_SQL = '''
WITH 
    RankedRatings AS 
    (SELECT author,
         published_date,
         rating,
         platform,
         rank()
        OVER (PARTITION BY author
    ORDER BY  published_date DESC) AS rnk
    FROM "blinkist"."ratings" ), 
    
    AvgRating AS 
    (SELECT published_date,
         platform,
         avg(rating) AS avgRating
    FROM RankedRatings
    WHERE rnk = 1
    GROUP BY  1,2), 
    
    AllCumulativeRating AS 
    (SELECT published_date,
         platform,
         avg(avgRating)
        OVER (PARTITION BY platform order by published_date ASC rows
        BETWEEN unbounded preceding
            AND current row) AS cumAvg
    FROM AvgRating )
    
SELECT a.published_date,
         a.avgRating,
         a.platform,     
    CASE
    WHEN a.avgRating < b.cumAvg THEN
    'Customer Rating Decreased'
    ELSE 'Customer Rating Increased'
    END
FROM AvgRating a
JOIN AllCumulativeRating b
    ON a.published_date = b.published_date and a.platform = b.platform
ORDER BY a.published_date;
'''


def create_client(service, region):
    return boto3.client(service, region_name=region)

def main():

    athena = create_client('athena', 'eu-west-1')
    resp = athena.start_query_execution(
        QueryString=AGGREGATE_SQL,
        QueryExecutionContext={
            'Database': 'blinkist'
        },
        ResultConfiguration={
            'OutputLocation': 's3://blinkist/aggregation/'
        },
        WorkGroup='primary'
    )
    print(resp["QueryExecutionId"])


if __name__ == "__main__":
    main()
