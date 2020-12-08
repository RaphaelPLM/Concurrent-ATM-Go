package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type account struct {
	id    int
	name  string
	value float64
}

type transaction struct {
	id              int
	value           float64
	transactionType int
	userID          int
}

type producer struct {
	id    int
	queue chan transaction
	quit  chan bool
}

var accountList = map[int]account{
	1: account{
		id:    1,
		name:  "Yuri",
		value: 200000.00,
	},
	2: account{
		id:    2,
		name:  "Raphael",
		value: 130000.00,
	},
	3: account{
		id:    3,
		name:  "Dalton",
		value: 150000.00,
	},
	4: account{
		id:    4,
		name:  "Maranhão",
		value: 170000.00,
	},
	5: account{
		id:    5,
		name:  "Fernando",
		value: 1.00,
	},
	6: account{
		id:    6,
		name:  "Cordeiro",
		value: 5000000.00,
	},
	7: account{
		id:    7,
		name:  "Christian",
		value: 20000.00,
	},
	8: account{
		id:    7,
		name:  "Paula",
		value: 27000.00,
	},
}

const consumerCount int = 50

var workers []*producer

var mapMutex = sync.RWMutex{}
var semaphores []*semaphore.Weighted


// Parse data from a CSV file using the CSVReader received as argument.
func produce(transactionChannel chan<- transaction, csvr *csv.Reader) {
	for {
		// Read a line from CSV
		row, err := csvr.Read()
		
		// Stop at EOF
		if err == io.EOF {
			break
		}

		// row[0] => transaction.id
		id, err := strconv.Atoi(row[0])
		if err != nil {
			log.Fatal(err)
		}

		// row[1] => transaction.value
		value, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			log.Fatal(err)
		}

		// row[2] => transaction.transactionType
		transactionType, err := strconv.Atoi(row[2])
		if err != nil {
			log.Fatal(err)
		}

		// row[3] => transaction.userID
		userID, err := strconv.Atoi(row[3])
		if err != nil {
			log.Fatal(err)
		}

		// Generate an instance of transaction struct
		transactionData := transaction{id: id, value: value, transactionType: transactionType, userID: userID}

		// Send transaction through channel, to be processed by consumers
		transactionChannel <- transactionData
	}

	// Close the transaction channel when CSV reading finishes
	close(transactionChannel)
}

func consume(consumerID int, transactionChannel <-chan transaction, done chan<- bool) {
	ctx := context.Background()

	// Process each transaction received in the channel
	for transactionData := range transactionChannel {
		
		// semaphore[i-1] indicates that the i-th user has an ongoing transaction, so other transactions regarding the same user must wait.
		semaphores[transactionData.userID-1].Acquire(ctx, 1)

		fmt.Printf("Consumer %v consuming transaction %v, value %v, user %v \n", consumerID, transactionData.id, transactionData.value, transactionData.userID)

		// Apply different processing based on transaction type
		switch transactionData.transactionType {
		case 1:
			withdraw(transactionData)
		case 2:
			deposit(transactionData)
		}

		fmt.Printf("Consumer %v finished consuming transaction %v, value %v, user %v \n", consumerID, transactionData.id, transactionData.value, transactionData.userID)

		// Release the semaphore of the i-th user
		semaphores[transactionData.userID-1].Release(1)
	}

	// Send signal through done channel to finish application
	done <- true
}

func withdraw(transactionData transaction) {
	// Simulate the processing time in a production enviromnent, that's increased by operations such as database r/w operations, encryption/decryption, etc...
	time.Sleep(time.Duration(rand.Intn(600)+401) * time.Millisecond)

	// Read user's account data under mutual exclusion with other processes
	mapMutex.RLock()
	accountData := accountList[transactionData.userID]
	mapMutex.RUnlock()

	// If user has sufficient funds, add transaction value to its account.
	if accountData.value >= transactionData.value {

		updatedAccountData := account{
			id:    accountData.id,
			name:  accountData.name,
			value: accountData.value - transactionData.value,
		}

		// Update account list with updated account under mutual exclusion with other processes
		mapMutex.Lock()
		accountList[transactionData.userID] = updatedAccountData
		mapMutex.Unlock()
	}
}

func deposit(transactionData transaction) {
	// Simulate the processing time in a production enviromnent, that's increased by operations such as database r/w operations, encryption/decryption, etc...
	time.Sleep(time.Duration(rand.Intn(600)+401) * time.Millisecond)

	// Read user's account data under mutual exclusion with other processes
	mapMutex.RLock()
	accountData := accountList[transactionData.userID]
	mapMutex.RUnlock()

	// Increment user funds with the transaction value
	updatedAccountData := account{
		id:    accountData.id,
		name:  accountData.name,
		value: accountData.value + transactionData.value,
	}

	mapMutex.Lock()
	accountList[transactionData.userID] = updatedAccountData
	mapMutex.Unlock()

}

func generateCSVReader(file string) *csv.Reader {
	f, err := os.Open(file)

	if err != nil {
		log.Fatal(err)
	}

	csvr := csv.NewReader(f)

	return csvr
}

func main() {
	fmt.Println("Initial state of account list:", accountList, "\n")

	done := make(chan bool)
	transactionChannel := make(chan transaction)

	csvr := generateCSVReader("result.csv")

	// Initialize one weighted semaphore (w/ weight 1) for each user
	for range accountList {
		semaphores = append(semaphores, semaphore.NewWeighted(1))
	}

	go produce(transactionChannel, csvr)

	for i := 0; i < consumerCount; i++ {
		go consume(i, transactionChannel, done)
	}

	<-done

	fmt.Println("\nFinal state of account list:", accountList)
}
