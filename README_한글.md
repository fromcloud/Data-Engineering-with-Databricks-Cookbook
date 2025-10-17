# Databricks를 활용한 데이터 엔지니어링 쿡북

<a href="https://www.packtpub.com/product/data-engineering-with-databricks-cookbook/9781837633357"><img src="https://content.packt.com/_/image/original/B19798/cover_image_large.jpg" alt="no-image" height="256px" align="right"></a>

이것은 Packt에서 출간한 [Databricks를 활용한 데이터 엔지니어링 쿡북](https://www.packtpub.com/product/data-engineering-with-databricks-cookbook/9781837633357)의 코드 저장소입니다.

**Apache Spark, Databricks, Delta Lake를 사용하여 효과적인 데이터 및 AI 솔루션 구축**

## 이 책은 무엇에 관한 책인가요?
이 책은 Apache Spark, Delta Lake, Databricks를 사용하여 데이터 파이프라인을 구축하고, 데이터를 관리 및 변환하며, 성능을 최적화하는 방법을 보여줍니다. 또한 DataOps 및 DevOps 관행을 구현하고 데이터 워크플로를 조정하는 방법도 다룹니다.

이 책은 다음과 같은 흥미로운 기능들을 다룹니다:
* Apache Spark를 사용한 데이터 로딩, 수집 및 처리 수행
* Apache Spark에서 데이터 변환 기법 및 사용자 정의 함수(UDF) 발견
* Apache Spark 및 Delta Lake API를 사용한 Delta 테이블 관리 및 최적화
* 실시간 데이터 처리를 위한 Spark Structured Streaming 사용
* Apache Spark 애플리케이션 및 Delta 테이블 쿼리 성능 최적화
* Databricks에서 DataOps 및 DevOps 관행 구현
* Delta Live Tables 및 Databricks Workflows를 사용한 데이터 파이프라인 조정
* Unity Catalog를 사용한 데이터 거버넌스 정책 구현

이 책이 당신에게 적합하다고 생각되면, 지금 [사본](https://www.amazon.com/Engineering-Apache-Spark-Delta-Cookbook/dp/1837633355)을 구입하세요!

<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" 
alt="https://www.packtpub.com/" border="5" /></a>

## 사용 방법 및 안내
모든 코드는 폴더별로 구성되어 있습니다. 예를 들어, Chapter01과 같습니다.

코드는 다음과 같이 보일 것입니다:
```
from pyspark.sql import SparkSession

spark = (SparkSession.builder
 .appName("read-csv-data")
 .master("spark://spark-master:7077")
 .config("spark.executor.memory", "512m")
 .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
```

**이 책을 위해 필요한 것들:**
이 책은 Apache Spark, Delta Lake, Databricks를 사용하여 효율적이고 확장 가능한 데이터 파이프라인을 구축하는 방법을 배우고자 하는 데이터 엔지니어, 데이터 사이언티스트, 데이터 실무자들을 위한 것입니다. 이 책을 최대한 활용하려면 데이터 아키텍처, SQL, Python 프로그래밍에 대한 기본 지식이 있어야 합니다.

다음 소프트웨어 및 하드웨어 목록으로 책에 있는 모든 코드 파일을 실행할 수 있습니다(1-11장).
### 소프트웨어 및 하드웨어 목록
| 챕터 | 필요한 소프트웨어 | 필요한 OS |
| -------- | ------------------------------------ | ----------------------------------- |
| 1-11 | Docker Engine 버전 18.02.0+ | Windows, Mac OS X, Linux (모든 버전) |
| 1-11 | Docker Compose 버전 1.25.5+ | Windows, Mac OS X, Linux (모든 버전) |
| 1-11 | Docker Desktop                 | Windows, Mac OS X, Linux (모든 버전) |
| 1-11 | Git                            | Windows, Mac OS X, Linux (모든 버전) |

### 관련 제품
* Databricks SQL을 활용한 비즈니스 인텔리전스 [[Packt]](https://www.packtpub.com/product/business-intelligence-with-databricks-sql/9781803235332) [[Amazon]](https://www.amazon.com/Business-Intelligence-Databricks-SQL-intelligence/dp/1803235330/ref=sr_1_1?crid=1QYCAOZP9E3NH&dib=eyJ2IjoiMSJ9.nKZ7dRFPdDZyRvWwKM_NiTSZyweCLZ8g9JdktemcYzaWNiGWg9PuoxY2yb2jogGyK8hgRliKebDQfdHu2rRnTZTWZbsWOJAN33k65RFkAgdFX-csS8HgTFfjZj-SFKLpp4FC6LHwQvWr9Nq6f5x6eg.jh99qre-Hl4OHA9rypXLmSGsQp4exBvaZ2xUOPDQ0mM&dib_tag=se&keywords=Business+Intelligence+with+Databricks+SQL&qid=1718173191&s=books&sprefix=business+intelligence+with+databricks+sql%2Cstripbooks-intl-ship%2C553&sr=1-1)

* Databricks 워크로드 최적화 [[Packt]](https://www.packtpub.com/product/optimizing-databricks-workloads/9781801819077) [[Amazon]](https://www.amazon.com/Optimizing-Databricks-Workloads-performance-workloads/dp/1801819076/ref=tmm_pap_swatch_0?_encoding=UTF8&dib_tag=se&dib=eyJ2IjoiMSJ9.cskfrEglx5gEbJF-FnhxlA.rCtKm1bO6Fi1mXUpq1Oai0kjAhGseGT2cCZ2Ccgxaak&qid=1718173341&sr=1-1)

## 저자 소개
**Pulkit Chadha**
는 데이터 엔지니어링 분야에서 15년 이상의 경험을 가진 숙련된 기술자입니다. 데이터 파이프라인을 제작하고 개선하는 그의 전문성은 의료, 미디어 및 엔터테인먼트, 하이테크, 제조업 등 다양한 분야에서 성공을 이끄는 데 중요한 역할을 했습니다. Pulkit의 맞춤형 데이터 엔지니어링 솔루션은 그가 협력하는 각 기업의 고유한 도전과 목표를 해결하도록 설계되었습니다.