package mysql

type AdsTagCopyModel struct {
	Id         int    `builder:"id"`
	AdId       int    `builder:"ad_id"`
	ContentTag string `builder:"content_tag"`
}

func AdsTagCopyQuery() *Query {
	return New("ads_tag_copy", "id", Database2)
}
