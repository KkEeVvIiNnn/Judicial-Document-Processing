# Judicial Document Processing

## doc sample
```json
"id": "1",
"doc_id": "42245f2a-c8d6-4ff4-a3e9-3eecb9e10cb3",
"source": varies in different type,
"crawl_time": "2018-09-18 12:04:33",
"status": "0",
"channel": "web",
"etl": null,
"d": "2018-09-18"
```

## Type1(0~80000000+\\40000000+)
The value of source is js code.
### keys in source
浏览, 文书ID, 案件名称, 案号, 审判程序, 上传日期, 案件类型, 补正文书, 法院名称, 法院ID, 法院省份, 法院地市, 法院区县, 法院区域, 文书类型, 文书全文类型, 裁判日期, 结案方式, 效力层级, 不公开理由, DocContent, 文本首部段落原文, 诉讼参与人信息部分原文, 诉讼记录段原文, 案件基本情况段原文, 裁判要旨段原文, 判决结果段原文, 附加原文, 案由, 当事人, LegalBase, Html

## Type2(40000000+~50000000+)
The value of source is a dict of three keys.
### keys in source
Title, PubDate, Html

## Type3(80000000+)
The value of source is a dict(Names of keys can be found in `quanwendic.js`).

## key list
案件名称, 法院名称, 效力层级, 文书类型, 案号, 案件类型, 审理程序, 审理程序, 案由, 案由, 案由, 案由, 案由, 案由, 当事人, 审判人员, 律师, 律所, 附加原文, 文本首部段落原文, 诉讼记录段原文, 诉控辩原文, 案件基本情况段原文, 理由原文, 判决结果段原文, 文本尾部原文, legal_base, 裁判日期, 不公开理由, 法院省份, 法院地市, 法院区县, pub_date, 公开类型, 关键字, 结案方式, legal_base, legal_base, 裁判日期, html, 浏览, 案件名称, pub_date, html