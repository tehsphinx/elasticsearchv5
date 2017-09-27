package elasticsearchv5

import (
	"context"
	"log"
	"strconv"

	"gopkg.in/olivere/elastic.v5"
)

func newSequence(client *Elastic) (*sequence, error) {
	s := &sequence{
		client:    client,
		cacheSize: client.sequenceCacheSize,
		docType:   client.index,
		id:        client.docType,
		ctx:       context.TODO(),
		cache:     make(chan int, client.sequenceCacheSize*2),
	}

	if err := s.checkSequenceIndex(); err != nil {
		return s, err
	}
	if err := s.checkMySequence(); err != nil {
		return s, err
	}

	go s.loadIDs(s.cacheSize)
	return s, nil
}

type sequence struct {
	client    *Elastic
	docType   string
	id        string
	cache     chan int
	cacheSize int
	ctx       context.Context
}

func (s *sequence) GetID() string {
	if len(s.cache) == 0 {
		log.Println("sequence cache is empty:", s.docType, s.id)
	}
	if len(s.cache) <= s.cacheSize {
		go s.loadIDs(s.cacheSize)
	}
	return strconv.Itoa(<-s.cache)
}

func (s *sequence) getIDs(size int) (*elastic.BulkResponse, error) {
	b := client.Bulk().Index("sequence").Type(s.docType)
	for i := 0; i < size; i++ {
		q := elastic.NewBulkIndexRequest().Id(s.id)
		b.Add(q)
	}

	return b.Do(s.ctx)
}

func (s *sequence) loadIDs(size int) {
	res, err := s.getIDs(size)
	if err != nil {
		log.Println(err)
	}
	for _, r := range res.Succeeded() {
		s.cache <- int(r.Version)
	}
}

func (s *sequence) getMaxID() (int, error) {
	res, err := s.client.Search(`{
		"query": {
			"match_all": {}
		},
		"size": 1,
		"sort": [
			{
				"_script": {
					"script": "doc['_uid'].value.length()",
					"type": "number",
					"order": "desc"
				},
				"_uid": {
					"order": "desc"
				}
			}
		]
	}`)
	if err != nil {
		return 0, err
	}

	var id = 0
	for _, hit := range res.Hits.Hits {
		id, err = strconv.Atoi(hit.Id)
		if err != nil {
			return 0, err
		}
	}
	return id, nil
}

func (s *sequence) checkSequenceIndex() error {
	if ok, err := s.client.IndexExists("sequence"); err != nil {
		return err
	} else if !ok {
		doc := `{
            "settings" : {
                "number_of_shards"     : 1,           
                "auto_expand_replicas" : "0-all"  
            },
            "mappings" : {
                "sequence" : {
                    "_source" : { "enabled" : 0 },
                    "_all"    : { "enabled" : 0 },
                    "enabled" : 0
                }
            }
        }`
		s.client.CreateIndex("sequence", doc)
	}

	return nil
}

func (s *sequence) checkMySequence() error {
	if ok, err := client.Exists().Index("sequence").Type(s.docType).Id(s.id).Do(s.ctx); err != nil {
		return err
	} else if !ok {
		lastID, err := s.getMaxID()
		if err != nil {
			return err
		}
		if 0 < lastID {
			err := s.writeLastID(lastID)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sequence) writeLastID(lastID int) error {
	_, err := s.getIDs(lastID)
	return err
}
