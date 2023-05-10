package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	outputFile := flag.String("output_file", "", "output file path")
	bucketName := flag.String("bucket_name", "", "name of the bucket")
	objectName := flag.String("object_name", "", "name of the object")
	accountID := flag.String("account_id", "", "account ID")
	accessKey := flag.String("access_key", "", "access key")
	secretKey := flag.String("secret_key", "", "secret key")
	flag.Parse()

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", *accountID),
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessKey, *secretKey, "")),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	fmt.Println("Downloading file, one moment.")
	bytesDownloaded, err := downloadFile(*outputFile, *bucketName, *objectName, client)
	if err != nil {
		log.Fatal("Unable to download file", err)
	}

	fmt.Println("Complete!")
	fmt.Printf("Successfully download %d bytes into %s\n", bytesDownloaded, *outputFile)
}

func downloadFile(filename, bucket, key string, client *s3.Client) (int64, error) {
	newFile, err := os.Create(filename)
	if err != nil {
		log.Println(err)
	}
	defer newFile.Close()

	downloader := manager.NewDownloader(client)
	numBytes, err := downloader.Download(context.TODO(), newFile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	return numBytes, err
}
