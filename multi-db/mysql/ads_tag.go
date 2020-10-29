package mysql

type AdsTagModel struct {
	Id         int    `builder:"id"`
	AdId       int    `builder:"ad_id"`
	ContentTag string `builder:"content_tag"`
}

func AdsTagQuery() *Query  {
	return New("ads_tags", "id", Database1)
}