package mediaworkflow

type MediaReview struct {
	ReviewID  int64  `json:"review_id"`
	UserID    int64  `json:"user_id"`
	ReqID     string `json:"req_id"`
	Text      string `json:"text"`
	MovieID   string `json:"movie_id"`
	Rating    int    `json:"rating"`
	Timestamp int64  `json:"timestamp"`
}

type MediaReviewIndexEntry struct {
	ReviewID  int64 `json:"review_id"`
	Timestamp int64 `json:"timestamp"`
}
