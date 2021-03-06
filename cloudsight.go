package main

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cloudsight/cloudsight-go"
	"github.com/parnurzeal/gorequest"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Config struct {
	CloudsightApiKey string
	MongoServer      string
	DatabaseName     string
	Debug            bool
	SlackChannel     string
}

const (
	ImageBaseUrl      = "http://cloudfront.mobmerry.com/store/"
	ProductCollection = "products"
	PictureCollection = "pictures"
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

type SlackMessage struct {
	Text string `json:"text"`
}

var doneChannel chan bool
var config Config
var cloudsightClient *cloudsight.Client

func main() {
	numCPUs := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPUs)

	if _, err := toml.DecodeFile("./secrets.toml", &config); err != nil {
		failOnError(err, "Unable to load config")
	}

	doneChannel = make(chan bool)
	currentTime := time.Now()

	PostToSlackChannel("[Cloudsight Sync] Started")
	go Process()

	if <-doneChannel {
		totalTime := time.Since(currentTime)
		log.Println("All Queries Completed")
		log.Println("Total Time - " + totalTime.String())
		PostToSlackChannel("[Cloudsight Sync] Time Elapsed -> " + totalTime.String())
		PostToSlackChannel("[Cloudsight Sync] Completed")
	}
}

func Process() {
	mongoDBDialInfo := &mgo.DialInfo{
		Addrs:     []string{config.MongoServer},
		Timeout:   60 * time.Second,
		Database:  config.DatabaseName,
		Direct:    true,
		PoolLimit: 100,
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

	log.Printf("%s: %v", "Total Pending", totalJobs)
	PostToSlackChannel("[Cloudsight Sync] Pending Jobs -> " + strconv.Itoa(totalJobs))

	go RunInBatches(jobsChannel, mongoSession, totalJobs)

	for message := range jobsChannel {
		completedJobs += message

		if completedJobs >= totalJobs {
			log.Printf("Total Jobs: %d\n", totalJobs)
			log.Printf("Completed Jobs: %d\n", completedJobs)
			PostToSlackChannel("[Cloudsight Sync] Completed Jobs -> " + strconv.Itoa(completedJobs))
			close(jobsChannel)
			doneChannel <- true
		}
	}
}

func PostToSlackChannel(msg string) {
	request := gorequest.New()
	msgJson := SlackMessage{Text: msg}

	resp, body, errs := request.Post(config.SlackChannel).Send(msgJson).End()
	if errs != nil {
		log.Fatalf("%s: %s", "Error while posting to Slack", errs)
		log.Println("Response : ", resp)
		log.Println("Response Body: ", body)
		return
	}
}

func FindPendingJobs(mongoSession *mgo.Session) int {
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	collection := sessionCopy.DB(config.DatabaseName).C(ProductCollection)

	var totalJobs int

	totalJobs, err := collection.Find(bson.M{"cloudsight_sync_at": bson.M{"$eq": nil}}).Count()

	if err != nil {
		log.Fatalf("%s: %s", "TotalPendingJobs Error", err)
		return 0
	}

	return totalJobs
}

func RunInBatches(jobsChannel chan int, mongoSession *mgo.Session, totalJobs int) {
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	collection := sessionCopy.DB(config.DatabaseName).C(ProductCollection)

	var products []Product
	cloudsightClient, _ = cloudsight.NewClientSimple(config.CloudsightApiKey)

	offset := 0
	limit := 1 // Running one product at a time due to cloudsight api rate limits :(

	for i := 0; i <= (totalJobs / limit); i++ {
		err := collection.Find(bson.M{"cloudsight_sync_at": bson.M{"$eq": nil}}).Limit(limit).Skip(offset).All(&products)

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

	pictureCollection := sessionCopy.DB(config.DatabaseName).C(PictureCollection)
	productCollection := sessionCopy.DB(config.DatabaseName).C(ProductCollection)

	log.Printf("Running For Product ID : %s\n", product.ID)

	err := pictureCollection.Find(bson.M{"image_resource_type": "Product", "image_resource_id": product.ID}).One(&picture)
	if err != nil {
		log.Fatalf("%s: %s", "Error while finding Picture", err)
		jobsChannel <- 1
		batchCountChannel <- 1
		return
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
		log.Fatalf("%s: %s", "Cloudsight Image Request Error", err)
		batchCountChannel <- 1
		jobsChannel <- 1
		return
	}

	err = cloudsightClient.WaitJob(job, 30*time.Second)
	if err != nil {
		log.Fatalf("%s: %s", "Cloudsight Image Response Error", err)
		batchCountChannel <- 1
		jobsChannel <- 1
		return
	}

	log.Println("Job Status : ", job.Status)
	log.Println("Job TTL : ", job.TTL)
	log.Println("Job Description : ", job.Status.Description())
	log.Printf("Job Token : %s\n", job.Token)
	log.Printf("Job Name : %v\n", job.Name)

	imageTags := strings.Fields(job.Name)
	err = productCollection.Update(bson.M{"_id": product.ID}, bson.M{"$addToSet": bson.M{"tags": bson.M{"$each": imageTags}}, "$set": bson.M{"cloudsight_sync_at": time.Now()}})
	if err != nil {
		log.Fatalf("%s: %s", "Error while updating Product", err)
	}

	batchCountChannel <- 1
	jobsChannel <- 1
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		PostToSlackChannel("[Cloudsight Sync] Error: " + msg)
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
