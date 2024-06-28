package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// OriginalRequest represents the incoming JSON structure
type OriginalRequest struct {
	Ev     string `json:"ev"`
	Et     string `json:"et"`
	ID     string `json:"id"`
	Uid    string `json:"uid"`
	Mid    string `json:"mid"`
	T      string `json:"t"`
	P      string `json:"p"`
	L      string `json:"l"`
	Sc     string `json:"sc"`
	Atrk1  string `json:"atrk1"`
	Atrv1  string `json:"atrv1"`
	Atrt1  string `json:"atrt1"`
	Atrk2  string `json:"atrk2"`
	Atrv2  string `json:"atrv2"`
	Atrt2  string `json:"atrt2"`
	Uatrk1 string `json:"uatrk1"`
	Uatrv1 string `json:"uatrv1"`
	Uatrt1 string `json:"uatrt1"`
	Uatrk2 string `json:"uatrk2"`
	Uatrv2 string `json:"uatrv2"`
	Uatrt2 string `json:"uatrt2"`
	Uatrk3 string `json:"uatrk3"`
	Uatrv3 string `json:"uatrv3"`
	Uatrt3 string `json:"uatrt3"`
}

// TransformedRequest represents the transformed JSON structure
type TransformedRequest struct {
	Event           string               `json:"event"`
	EventType       string               `json:"event_type"`
	AppID           string               `json:"app_id"`
	UserID          string               `json:"user_id"`
	MessageID       string               `json:"message_id"`
	PageTitle       string               `json:"page_title"`
	PageURL         string               `json:"page_url"`
	BrowserLanguage string               `json:"browser_language"`
	ScreenSize      string               `json:"screen_size"`
	Attributes      map[string]Attribute `json:"attributes"`
	Traits          map[string]Trait     `json:"traits"`
}

// Attribute represents an attribute with its value and type
type Attribute struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

// Trait represents a trait with its value and type
type Trait struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

func main() {
	// Create a channel to send data from HTTP handler to worker
	dataChannel := make(chan OriginalRequest)

	// Start the worker
	go worker(dataChannel)

	// Set up the HTTP server
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var originalRequest OriginalRequest
		if err := json.NewDecoder(r.Body).Decode(&originalRequest); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Send the request to the worker via the channel
		dataChannel <- originalRequest

		// Send a response to the client
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Request received"))
	})

	log.Println("HTTP server started on port 8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func worker(dataChannel chan OriginalRequest) {
	for originalRequest := range dataChannel {
		// Transform the original request
		transformedRequest := TransformedRequest{
			Event:           originalRequest.Ev,
			EventType:       originalRequest.Et,
			AppID:           originalRequest.ID,
			UserID:          originalRequest.Uid,
			MessageID:       originalRequest.Mid,
			PageTitle:       originalRequest.T,
			PageURL:         originalRequest.P,
			BrowserLanguage: originalRequest.L,
			ScreenSize:      originalRequest.Sc,
			Attributes: map[string]Attribute{
				originalRequest.Atrk1: {Value: originalRequest.Atrv1, Type: originalRequest.Atrt1},
				originalRequest.Atrk2: {Value: originalRequest.Atrv2, Type: originalRequest.Atrt2},
			},
			Traits: map[string]Trait{
				originalRequest.Uatrk1: {Value: originalRequest.Uatrv1, Type: originalRequest.Uatrt1},
				originalRequest.Uatrk2: {Value: originalRequest.Uatrv2, Type: originalRequest.Uatrt2},
				originalRequest.Uatrk3: {Value: originalRequest.Uatrv3, Type: originalRequest.Uatrt3},
			},
		}

		// Log the transformed request (or send it to another service, etc.)
		transformedRequestJSON, err := json.Marshal(transformedRequest)
		if err != nil {
			log.Printf("Failed to marshal transformed request: %v", err)
			continue
		}

		log.Printf("Transformed request: %s", transformedRequestJSON)
	}
}
