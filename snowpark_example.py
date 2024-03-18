# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

# Note: This requires the DB, Schema and tables to be setup!

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import month,year,col,sum

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    # tableName = 'information_schema.packages'
    # dataframe = session.table(tableName).filter(col("language") == 'python')

    snow_df_spend = session.table("campaign_spend")
    snow_df_revenue = session.table("monthly_revenue")

    # Total Spend per Year and Month For All Channels
    snow_df_spend_per_channel = snow_df_spend \
        .group_by(year('DATE'), month('DATE'),'CHANNEL')\
        .agg(sum('TOTAL_COST').as_('TOTAL_COST'))\
        .with_column_renamed('"YEAR(DATE)"',"YEAR")\
        .with_column_renamed('"MONTH(DATE)"',"MONTH")\
        .sort('YEAR','MONTH')
    
    print("Total Spend per Year and Month For All Channels")
    snow_df_spend_per_channel.show()
    
    # Total Spend Across All Channels
    snow_df_spend_per_month = \
        snow_df_spend_per_channel.pivot('CHANNEL',['search_engine','social_media','video','email'])\
        .sum('TOTAL_COST').sort('YEAR','MONTH')
    
    snow_df_spend_per_month = snow_df_spend_per_month.select(
        col("YEAR"),
        col("MONTH"),
        col("'search_engine'").as_("SEARCH_ENGINE"),
        col("'social_media'").as_("SOCIAL_MEDIA"),
        col("'video'").as_("VIDEO"),
        col("'email'").as_("EMAIL")
    )

    print("Total Spend Across All Channels")
    snow_df_spend_per_month.show()

    # Total Revenue per Year and Month Data
    snow_df_revenue_per_month = snow_df_revenue.group_by('YEAR','MONTH')\
        .agg(sum('REVENUE')).sort('YEAR','MONTH')\
        .with_column_renamed('SUM(REVENUE)','REVENUE')

    print("Total Revenue per Year and Month")
    snow_df_revenue_per_month.show()

    # Join Total Spend and Total Revenue per Year and Month Across All Channels
    snow_df_spend_and_revenue_per_month = \
        snow_df_spend_per_month.join(snow_df_revenue_per_month, ["YEAR","MONTH"])
    
    print("Total Spend and Revenue per Year and Month Across All Channels")
    snow_df_spend_and_revenue_per_month.show()

    # Examine Query Explain Plan

    snow_df_spend_and_revenue_per_month.explain()

    # Save transformed data
    snow_df_spend_and_revenue_per_month.write.mode('overwrite')\
        .save_as_table('SPEND_AND_REVENUE_PER_MONTH')
    
    return snow_df_spend_and_revenue_per_month
