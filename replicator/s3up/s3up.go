/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package s3up

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const BUFFERSIZE = 5242880

// set it to TRUE to enable sha256 part checksum (?? must be also enabled on s3??)
var (
	UseSHA256 = false
	debug     *log.Logger
	Loglevel  = 0
)

type uploadPart struct {
	XMLName    xml.Name `xml:"Part"`
	Sha256sum  string   `xml:"ChecksumSHA256,omitempty"`
	ETag       string   `xml:"ETag,omitempty"`
	PartNumber int      `xml:"PartNumber"`
}

type multipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []*uploadPart
}

type errorResp struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

type S3Uploader struct {
	ctx        context.Context
	location   string
	access_key string
	secret_key string
	client     *http.Client
	uploadId   string
	//etags      []string
	buffer    []byte
	nextPn    int
	objKey    string
	bucket    string
	parts     multipartUpload
	useSha256 bool
}

func (s *S3Uploader) SignedRequestV4(
	reqUrl string,
	method string,
	body io.Reader,
	contentType string,
	contentSha256 []byte,
	setupRequest func(req *http.Request) error,
	t time.Time,
) (
	*http.Request,
	error,
) {
	const authorization = "AWS4-HMAC-SHA256"
	const unsignedPayload = "UNSIGNED-PAYLOAD"
	const serviceName = "s3"

	req, err := http.NewRequestWithContext(s.ctx, method, reqUrl, body)
	if err != nil {
		return nil, err
	}
	err = setupRequest(req)
	if err != nil {
		return nil, err
	}

	timeISO8601 := t.Format("20060102T150405Z")
	timeYYYYMMDD := t.Format("20060102")
	scope := timeYYYYMMDD + "/" + s.location + "/" + serviceName + "/aws4_request"
	credential := s.access_key + "/" + scope

	contentSha256_string := unsignedPayload
	if contentSha256 != nil {
		contentSha256_string = hex.EncodeToString(contentSha256[:])
		if s.useSha256 {
			b64_shasum := base64.StdEncoding.EncodeToString(contentSha256[:])
			req.Header.Set("x-amz-checksum-sha256", b64_shasum)
		}
	}

	req.Header.Set("X-Amz-Date", timeISO8601)
	req.Header.Set("X-Amz-Content-Sha256", contentSha256_string)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	canonicalURI := req.URL.Path // TODO: This may require some encoding
	canonicalQueryString := req.URL.Query().Encode()

	signerHeadersList := []string{"host"}
	for h := range req.Header {
		signerHeadersList = append(signerHeadersList, strings.ToLower(h))
	}
	sort.Strings(signerHeadersList)
	signedHeaders := strings.Join(signerHeadersList, ";")
	canonicalHeaders := ""
	for _, h := range signerHeadersList {
		if h == "host" {
			canonicalHeaders = canonicalHeaders + h + ":" + req.Host + "\n"
		} else {
			canonicalHeaders = canonicalHeaders + h + ":" + req.Header.Get(h) + "\n"
		}
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		contentSha256_string,
	}, "\n")
	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))

	stringToSign := authorization + "\n" +
		timeISO8601 + "\n" +
		scope + "\n" +
		hex.EncodeToString(canonicalRequestHash[:])

	hmacSha256 := func(key []byte, data []byte) []byte {
		h := hmac.New(sha256.New, key)
		h.Write(data)
		return h.Sum(nil)
	}

	dateKey := hmacSha256([]byte("AWS4"+s.secret_key), []byte(timeYYYYMMDD))
	dateRegionKey := hmacSha256(dateKey, []byte(s.location))
	dateRegionServiceKey := hmacSha256(dateRegionKey, []byte(serviceName))
	signingKey := hmacSha256(dateRegionServiceKey, []byte("aws4_request"))

	signature := hex.EncodeToString(hmacSha256(signingKey, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s,SignedHeaders=%s,Signature=%s",
		authorization,
		credential,
		signedHeaders,
		signature,
	))

	return req, nil
}

type chunkedUploadResponse struct {
	Res      xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

func Open(key, bucket, region, access_key, secret_key string) (s3 *S3Uploader, err error) {
	uri := fmt.Sprintf("https://%s.s3.amazonaws.com/%s?uploads", bucket, key)

	s3 = &S3Uploader{
		ctx:        context.Background(),
		location:   region,
		access_key: access_key,
		secret_key: secret_key,
		client:     &http.Client{},
		nextPn:     1,
		objKey:     key,
		bucket:     bucket,
		parts:      multipartUpload{},
		buffer:     nil,
		useSha256:  UseSHA256,
	}
	req, err := s3.SignedRequestV4(uri, "POST", nil, "", nil, func(req *http.Request) error { return nil }, time.Now().UTC())
	if err != nil {
		debug.Printf("Unable to build req: %s", err.Error())
		return nil, err
	}
	debug.Printf("auth: %+v", req.Header)
	resp, err := s3.client.Do(req)
	if err != nil {
		debug.Printf("Unable to exec req: %s", err.Error())
		return nil, err
	}
	debug.Printf("Status %s", resp.Status)
	debug.Printf("Headers %+v", resp.Header)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		debug.Printf("Unable to read response body: %s", err.Error())
		return nil, err
	}
	debug.Printf("Resp body: %s", body)
	if resp.StatusCode != 200 {
		debug.Printf("Authentication failed: %s", body)
		var errorResponse errorResp
		errmsg := "?? [unable to parse response]"
		err = xml.Unmarshal(body, &errorResponse)
		if err == nil {
			errmsg = fmt.Sprintf("%s [%s]", errorResponse.Message, errorResponse.Code)
		}
		return nil, fmt.Errorf("Failed authentication: %s", errmsg)
	}
	var xmlResp chunkedUploadResponse
	err = xml.Unmarshal(body, &xmlResp)
	if err != nil {
		debug.Printf("Unable to decode response: %s", err.Error())
		return nil, err
	}
	debug.Printf("Parsed xml body: %+v", xmlResp)
	s3.uploadId = xmlResp.UploadId
	log.Printf("S3: uploading to s3://%s/%s", s3.bucket, s3.objKey)
	return s3, nil
}

func (s3 *S3Uploader) Write(p []byte) (int, error) {
	totw := 0
	for {
		w, err := s3.Write1(p[totw:])
		totw = totw + w
		if err != nil || len(p) == totw {
			return totw, err
		}
	}
}

func (s3 *S3Uploader) Write1(p []byte) (n int, err error) {
	ava := BUFFERSIZE - len(s3.buffer)
	len_p := len(p)
	if len_p < ava {
		s3.buffer = append(s3.buffer, p...)
		return len_p, nil
	}
	s3.buffer = append(s3.buffer, p[:ava]...)
	err = s3.Flush()
	if err != nil {
		return 0, err
	}
	return ava, nil
}

func extract_etag(headers map[string][]string) string {
	etag := ""
	etag_list, ok := headers["Etag"]
	if ok && len(etag_list) > 0 {
		etag = etag_list[0]
	}
	return etag
}

func (s3 *S3Uploader) Flush() (err error) {
	log.Printf("S3: uploading %d bytes to s3://%s/%s", len(s3.buffer), s3.bucket, s3.objKey)
	debug.Printf("Writing %d bytes: %v", len(s3.buffer), s3.buffer[0:8])
	shasum := sha256.Sum256(s3.buffer)
	uri := fmt.Sprintf("https://%s.s3.amazonaws.com/%s?partNumber=%d&uploadId=%s", s3.bucket, s3.objKey, s3.nextPn, s3.uploadId)
	req, err := s3.SignedRequestV4(uri, "PUT", bytes.NewReader(s3.buffer), "", shasum[:], func(req *http.Request) error { return nil }, time.Now().UTC())
	if err != nil {
		debug.Printf("Unable to build req: %s", err.Error())
		return err
	}
	resp, err := s3.client.Do(req)
	debug.Printf("Status %s", resp.Status)
	debug.Printf("Headers %+v", resp.Header)
	if err != nil {
		debug.Printf("Unable to commit req: %s", err.Error())
		return err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		debug.Printf("Unable to read response body: %s", err.Error())
		return err
	}
	debug.Printf("Body %s", string(bodyBytes))
	newPart := uploadPart{
		PartNumber: s3.nextPn,
		ETag:       extract_etag(resp.Header),
	}
	if s3.useSha256 {
		newPart.Sha256sum = base64.StdEncoding.EncodeToString(shasum[:])
	}
	s3.parts.Parts = append(s3.parts.Parts, &newPart)
	s3.nextPn += 1
	s3.buffer = nil
	return nil
}

func (s3 *S3Uploader) Close() (err error) {
	if len(s3.buffer) > 0 {
		s3.Flush()
	}
	uri := fmt.Sprintf("https://%s.s3.amazonaws.com/%s?uploadId=%s", s3.bucket, s3.objKey, s3.uploadId)
	closeBody, _ := xml.Marshal(s3.parts)
	req, err := s3.SignedRequestV4(uri, "POST", bytes.NewReader([]byte(closeBody)), "", nil, func(req *http.Request) error { return nil }, time.Now().UTC())
	if err != nil {
		debug.Printf("Unable to build req: %s", err.Error())
		return err
	}
	resp, err := s3.client.Do(req)
	debug.Printf("Status %s", resp.Status)
	debug.Printf("Headers %+v", resp.Header)
	if err != nil {
		debug.Printf("Unable to commit req: %s", err.Error())
		return err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		debug.Printf("Unable to read response body: %s", err.Error())
		return err
	}
	debug.Printf("Body: %s", string(bodyBytes))
	log.Printf("S3: upload to s3://%s/%s ended", s3.bucket, s3.objKey)
	return nil
}

func init() {
	if Loglevel > 0 {
		debug = log.New(os.Stderr, "DEBUG: ", log.LstdFlags|log.Lshortfile)
	} else {
		debug = log.New(ioutil.Discard, "DEBUG: ", log.LstdFlags|log.Lshortfile)
	}
}
