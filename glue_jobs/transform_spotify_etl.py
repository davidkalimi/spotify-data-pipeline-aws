import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node tracks
tracks_node1744828835465 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-soptify-datewithdata/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1744828835465")

# Script generated for node artist
artist_node1744828835029 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-soptify-datewithdata/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1744828835029")

# Script generated for node album
album_node1744828833443 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-soptify-datewithdata/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1744828833443")

# Script generated for node Join Album & Artist
JoinAlbumArtist_node1744828914743 = Join.apply(frame1=album_node1744828833443, frame2=artist_node1744828835029, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumArtist_node1744828914743")

# Script generated for node Join with tracks
Joinwithtracks_node1744829012295 = Join.apply(frame1=tracks_node1744828835465, frame2=JoinAlbumArtist_node1744828914743, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtracks_node1744829012295")

# Script generated for node Drop Fields
DropFields_node1744829084559 = DropFields.apply(frame=Joinwithtracks_node1744829012295, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1744829084559")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1744829084559, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744828821537", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1744829134277 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1744829084559, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-soptify-datewithdata/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1744829134277")

job.commit()