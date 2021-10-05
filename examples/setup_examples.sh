#!/bin/sh

ROOT_PATH=$(git rev-parse --show-toplevel)

cd $ROOT_PATH

EXAMPLES_FOLDER=$(git rev-parse --show-toplevel)/examples
METASTORE=$EXAMPLES_FOLDER/metastore

EXAMPLES_WIKIPEDIA_FOLDER=$EXAMPLES_FOLDER/wikipedia
INDEX_CONFIG_WIKIPEDIA=$EXAMPLES_FOLDER/index_configs/wikipedia_index_config.json

EXAMPLES_HDFS_FOLDER=$EXAMPLES_FOLDER/hdfs
INDEX_CONFIG_HDFS=$EXAMPLES_FOLDER/index_configs/hdfslogs_index_config.json

clean_all()
{
    [ -d $METASTORE ] && rm -r $METASTORE 
    [ -d $EXAMPLES_WIKIPEDIA_FOLDER ] && rm -r $EXAMPLES_WIKIPEDIA_FOLDER 
    [ -d $EXAMPLES_HDFS_FOLDER ] && rm -r $EXAMPLES_HDFS_FOLDER
}

setup_wikipedia()
{
    [ -d $EXAMPLES_WIKIPEDIA_FOLDER ] && rm -r $EXAMPLES_WIKIPEDIA_FOLDER 
    [ ! -d $EXAMPLES_WIKIPEDIA_FOLDER ] && mkdir $EXAMPLES_WIKIPEDIA_FOLDER 
    # Setup wikipedia example
    cargo run -- new --index-uri file://$EXAMPLES_WIKIPEDIA_FOLDER --index-config-path $INDEX_CONFIG_WIKIPEDIA --metastore-uri file://$METASTORE
    # Download the first 10_000 Wikipedia articles.
    curl -o $EXAMPLES_WIKIPEDIA_FOLDER/wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json
    # Index the dataset
    cargo run -- index --index-id wikipedia --metastore-uri file://$METASTORE --input-path $EXAMPLES_WIKIPEDIA_FOLDER/wiki-articles-10000.json
}

setup_hdfs()
{
    [ -d $EXAMPLES_HDFS_FOLDER ] && rm -r $EXAMPLES_HDFS_FOLDER
    [ ! -d $EXAMPLES_HDFS_FOLDER ] && mkdir $EXAMPLES_HDFS_FOLDER
    # Setup hdfs examplE
    cargo run -- new --index-uri file://$EXAMPLES_HDFS_FOLDER --index-config-path $INDEX_CONFIG_HDFS --metastore-uri file://$METASTORE
    # Download the dataset
    curl https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz | gunzip > $EXAMPLES_HDFS_FOLDER/hdfs.logs.quickwit.json
    # Index the dataset
    cargo run -- index --index-id hdfs --metastore-uri file://$METASTORE --input-path $EXAMPLES_HDFS_FOLDER/hdfs.logs.quickwit.json
}

case $1 in 
    wikipedia)
        setup_wikipedia
        break
        ;;
    hdfs)
        setup_hdfs
        break
        ;;

    remove-all)
        clean_all
        break
        ;;
    *)
        echo "You can choose either wikipedia or hdfs or you can remove everything using remove-all"
        break
        ;;
esac
     
