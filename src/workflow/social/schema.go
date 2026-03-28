package socialworkflow

type Post struct {
	PostID    string `json:"post_id"`
	Timestamp int64  `json:"timestamp"`
	Text      string `json:"text"`
	CreatorID string `json:"creator_id"`
}

type SocialGraphEntry struct {
	UserID    string   `json:"user_id"`
	Followers []string `json:"followers"`
	Followees []string `json:"followees"`
}
