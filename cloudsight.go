package main

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cloudsight/cloudsight-go"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Config struct {
	CloudsightApiKey string
	MongoServer      string
	DatabaseName     string
	NumOfDays        int
	Debug            bool
}

const (
	ImageBaseUrl = "http://cloudfront.mobmerry.com/store/"
)

type Product struct {
	ID        bson.ObjectId `bson:"_id"`
	UpdatedAt time.Time     `bson:"updated_at"`
}

type Picture struct {
	ID           bson.ObjectId `bson:"_id"`
	ResourceId   bson.ObjectId `bson:"image_resource_id"`
	ResourceType string        `bson:"image_resource_type"`
	FileData     string        `bson:"file_data"`
}

type DefaultVersionFile struct {
	ID string `json:"id"`
}

type FileDataType struct {
	Key DefaultVersionFile `json:"default"`
}

var doneChannel chan bool
var config Config
var cloudsightClient *cloudsight.Client
var startDate time.Time

func main() {
	numCPUs := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPUs)

	if _, err := toml.DecodeFile("./secrets.toml", &config); err != nil {
		failOnError(err, "Unable to load config")
	}

	doneChannel = make(chan bool)

	currentTime := time.Now()
	startDate = currentTime.AddDate(0, 0, config.NumOfDays)

	go Process()

	if <-doneChannel {
		log.Println("All Queries Completed")
		log.Println(time.Since(currentTime))
	}
}

func Process() {
	mongoDBDialInfo := &mgo.DialInfo{
		Addrs:    []string{config.MongoServer},
		Timeout:  60 * time.Second,
		Database: config.DatabaseName,
	}

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	mongoSession, err := mgo.DialWithInfo(mongoDBDialInfo)

	failOnError(err, "Can't connect to mongo")

	mongoSession.SetMode(mgo.Monotonic, true)
	mgo.SetDebug(config.Debug)

	var mongoLogger *log.Logger
	mongoLogger = log.New(os.Stderr, "[MongoDB]", log.LstdFlags)
	mgo.SetLogger(mongoLogger)

	jobsChannel := make(chan int)
	var completedJobs int

	totalJobs := FindPendingJobs(mongoSession)

	go RunInBatches(jobsChannel, mongoSession, totalJobs)

	for message := range jobsChannel {
		completedJobs += message

		if completedJobs >= totalJobs {
			log.Printf("Total Jobs: %d\n", totalJobs)
			log.Printf("Completed Jobs: %d\n", completedJobs)
			close(jobsChannel)
			doneChannel <- true
		}
	}
}

func FindPendingJobs(mongoSession *mgo.Session) int {
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	collection := sessionCopy.DB(config.DatabaseName).C("pictures")

	var totalJobs int

	totalJobs, err := collection.Find(bson.M{"image_resource_type": "Product", "created_at": bson.M{"$gte": startDate}}).Count()

	if err != nil {
		log.Fatalf("%s: %s", "TotalPendingJobs Error", err)
		return 0
	}

	return totalJobs
}

func RunInBatches(jobsChannel chan int, mongoSession *mgo.Session, totalJobs int) {
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	collection := sessionCopy.DB(config.DatabaseName).C("products")

	var products []Product
	var cloudsightErr error

	cloudsightClient, cloudsightErr = cloudsight.NewClientSimple(config.CloudsightApiKey)
	failOnError(cloudsightErr, "Error while creating CloughtSight Client")

	offset := 0
	limit := 1 // Running one product at a time due to cloudsight api rate limits :(

	log.Println(totalJobs)

	for i := 0; i <= (totalJobs / limit); i++ {
		err := collection.Find(bson.M{"created_at": bson.M{"$gte": startDate}}).Limit(limit).Skip(offset).All(&products)

		if err != nil {
			log.Fatalf("%s: %s", "RunQuery Error", err)
			doneChannel <- true
		}

		if len(products) == 0 {
			log.Println("No Products Found")
			doneChannel <- true
		}

		batchCompleteChannel := make(chan bool)
		batchCountChannel := make(chan int)
		rateLimit := 0

		for _, product := range products {
			go syncWithCloudsight(product, mongoSession, jobsChannel, batchCountChannel)
		}

		go func() {
			for message := range batchCountChannel {
				rateLimit += message

				log.Printf("Rate Limit - %v", rateLimit)
				if rateLimit >= limit {
					batchCompleteChannel <- true
				}
			}
		}()

		offset += limit
		<-batchCompleteChannel
	}
}

func syncWithCloudsight(product Product, mongoSession *mgo.Session, jobsChannel chan int, batchCountChannel chan int) {
	var picture Picture
	var fileData FileDataType

	stop := startTimer("Cloudsight Sync")
	defer stop()

	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	pictureCollection := sessionCopy.DB(config.DatabaseName).C("pictures")
	productCollection := sessionCopy.DB(config.DatabaseName).C("products")

	log.Printf("Running For Product ID : %s\n", product.ID)

	err := pictureCollection.Find(bson.M{"image_resource_type": "Product", "image_resource_id": product.ID}).One(&picture)
	if err != nil {
		log.Fatalf("%s: %s", "Error while finding Picture", err)
		jobsChannel <- 1
		batchCountChannel <- 1
	}

	fileDataInBytes := []byte(picture.FileData)

	if err = json.Unmarshal(fileDataInBytes, &fileData); err != nil {
		log.Fatalf("%s: %s", "Unmarshal Error", err)
		jobsChannel <- 1
		batchCountChannel <- 1
		return
	}

	imageUrl := ImageBaseUrl + fileData.Key.ID

	log.Printf("Product URL : %s\n", imageUrl)

	params := cloudsight.Params{}
	params.SetLocale("en")

	job, err := cloudsightClient.RemoteImageRequest(imageUrl, params)
	if err != nil {
		log.Fatalf("%s: %s", "Cloudsight Error", err)
		jobsChannel <- 1
		batchCountChannel <- 1
		return
	}

	err = cloudsightClient.WaitJob(job, 15*time.Second)
	if err != nil {
		log.Fatalf("%s: %s", "Cloudsight Error", err)
		jobsChannel <- 1
		batchCountChannel <- 1
		return
	}

	log.Println("Job Status : ", job.Status)
	log.Println("Job TTL : ", job.TTL)
	log.Println("Job Description : ", job.Status.Description())
	log.Printf("Job Token : %s\n", job.Token)
	log.Printf("Job Name : %v\n", job.Name)

	imageTags := strings.Fields(job.Name)
	err = productCollection.Update(bson.M{"_id": product.ID}, bson.M{"$addToSet": bson.M{"tags": bson.M{"$each": imageTags}}, "$set": bson.M{"updated_at": time.Now()}})
	if err != nil {
		log.Fatalf("%s: %s", "Error while updating Product", err)
	}

	batchCountChannel <- 1
	jobsChannel <- 1
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		doneChannel <- true
	}
}

func startTimer(name string) func() {
	t := time.Now()

	log.Println(name, "Started")
	return func() {
		d := time.Now().Sub(t)
		log.Println(name, "Took", d)
	}
}
