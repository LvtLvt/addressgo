package meta

var CsvHead = map[string][]string{
	"주소": {"관리번호", "도로명코드", "읍면동일련번호", "지하여부", "건물본번", "건물부번", "기초구역번호", "변경사유코드", "고시일자", "변경전도로명주소", "상세주소부여여부"},
	"개선": {"도로명코드", "도로명", "도로명로마자", "읍면동일련번호", "시도명", "시도로마자", "시군구명", "시군구로마자",
		"읍면동명", "읍면동로마자", "읍면동구분", "읍면동코드", "사용여부", "변경사유", "변경이력정보", "고시일자", "말소일자"},
	"부가정보": {"관리번호", "행정동코드", "행정동명", "우편번호", "우편번호일련번호", "다량배달처명", "건축물대장건물명", "시군구건물명", "공동주택여부"},
	"지번":   {"관리번호", "일련번호", "법정동코드", "시도명", "시군구명", "법정읍면동명", "법정리명", "산여부", "지번본번", "지번부번", "대표여부"},
}

const (
	Jibun_관리번호 = iota
	Jibun_일련번호
	Jibun_법정동코드
	Jibun_시도명
	JIbun_시군구명
	Jibun_법정읍면동명
	JIbun_법정리명
	Jibun_산여부
	Jibun_지번본번
	Jibun_지번부번
	Jibun_대표여부
)

const (
	Buga_관리번호 = iota
	Buga_행정동코드
	Buga_행정동명
	Buga_우편번호
	Buga_우편번호일련번호
	Buga_다량배달처명
	Buga_건축물대장건물명
	Buga_시군구건물명
	Buga_공동주택여부
)

const (
	Doro_도로명코드 = iota
	Doro_도로명
	Doro_도로명로마자
	Doro_읍면동일련번호
	Doro_시도명
	Doro_시도로마자
	Doro_시군구명
	Doro_시군구로마자
	Doro_읍면동명
	Doro_읍면동로마자
	Doro_읍면동구분
	Doro_읍면동코드
	Doro_사용여부
	Doro_변경사유
	Doro_변경이력정보
	Doro_고시일자
	Doro_말소일자
)

const (
	Juso_관리번호 = iota
	Juso_도로명코드
	Juso_읍면동일련번호
	Juso_지하여부
	Juso_건물본번
	Juso_건물부번
	Juso_기초구역번호
	Juso_변경사유코드
	Juso_고시일자
	Juso_변경전도로명주소
	Juso_상세주소부여여부
)

type Juso struct {
	A_관리번호     string `csv:"관리번호"`
	A_도로명코드    string `csv:"도로명코드"`
	A_읍면동일련번호  string `csv:"읍면동일련번호"`
	A_지하여부     string `csv:"지하여부"`
	A_건물본번     string `csv:"건물본번"`
	A_건물부번     string `csv:"건물부번"`
	A_기초구역번호   string `csv:"기초구역번호"`
	A_변경사유코드   string `csv:"변경사유코드"`
	A_고시일자     string `csv:"고시일자"`
	A_변경전도로명주소 string `csv:"변경전도로명주소"`
	A_상세주소부여여부 string `csv:"상세주소부여여부"`
}
type Doro struct {
	A_도로명코드   string `csv:"도로명코드"`
	A_도로명     string `csv:"도로명"`
	A_도로명로마자  string `csv:"도로명로마자"`
	A_읍면동일련번호 string `csv:"읍면동일련번호"`
	A_시도명     string `csv:"시도명"`
	A_시도로마자   string `csv:"시도로마자"`
	A_시군구명    string `csv:"시군구명"`
	A_시군구로마자  string `csv:"시군구로마자"`
	A_읍면동명    string `csv:"읍면동명"`
	A_읍면동로마자  string `csv:"읍면동로마자"`
	A_읍면동구분   string `csv:"읍면동구분"`
	A_읍면동코드   string `csv:"읍면동코드"`
	A_사용여부    string `csv:"사용여부"`
	A_변경사유    string `csv:"변경사유"`
	A_변경이력정보  string `csv:"변경이력정보"`
	A_고시일자    string `csv:"고시일자"`
	A_말소일자    string `csv:"말소일자"`
}
type Buga struct {
	A_관리번호     string `csv:"관리번호"`
	A_행정동코드    string `csv:"행정동코드"`
	A_행정동명     string `csv:"행정동명"`
	A_우편번호     string `csv:"우편번호"`
	A_우편번호일련번호 string `csv:"우편번호일련번호"`
	A_다량배달처명   string `csv:"다량배달처명"`
	A_건축물대장건물명 string `csv:"건축물대장건물명"`
	A_시군구건물명   string `csv:"시군구건물명"`
	A_공동주택여부   string `csv:"공동주택여부"`
}
type Jibun struct {
	A_관리번호   string `csv:"관리번호"`
	A_일련번호   string `csv:"일련번호"`
	A_법정동코드  string `csv:"법정동코드"`
	A_시도명    string `csv:"시도명"`
	A_시군구명   string `csv:"시군구명"`
	A_법정읍면동명 string `csv:"법정읍면동명"`
	A_법정리명   string `csv:"법정리명"`
	A_산여부    string `csv:"산여부"`
	A_지번본번   string `csv:"지번본번"`
	A_지번부번   string `csv:"지번부번"`
	A_대표여부   string `csv:"대표여부"`
}
