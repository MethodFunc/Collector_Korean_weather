# 스케줄러 수집기
## 0. 설명
해당 코드는 API 및 크롤링을 이용하여 기상청과 바다누리의 데이터를 수집하는 코드입니다.

## 1. API발급 주소
두 API모두 회원가입 필수
1. 기상청 ASOS (시간): https://www.data.go.kr/data/15057210/openapi.do
2. 바다누리: http://www.khoa.go.kr/oceangrid/khoa/takepart/openapi/openApiKey.do

## 2. 크롤링
크롤링 허가 여부
~~~
User-Agent: *
Allow: /
~~~

## 3. 필수 라이브러리
~~~
pymongo
pandas
numpy
apscheduler
pyymal
~~~

## 4. 사용방법
~~~
python collectable.py -t aws/asos/badanuri
~~~


## 5. 
m1 mac - 작동이 안됨. (확인 중)

윈도우 - 작동 확인 완료.

리눅스 - 없음