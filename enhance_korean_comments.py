#!/usr/bin/env python3
"""
Databricks 노트북 파일들에 더 상세한 한글 주석을 추가하는 개선된 스크립트
"""

import os
import json
import re
from pathlib import Path

def enhance_korean_comments_in_notebook(notebook_path):
    """노트북 파일에 더 상세한 한글 주석을 추가하는 함수"""
    
    # 더 포괄적인 주석 매핑
    enhanced_mappings = {
        # Delta Lake 관련
        r'from delta import': 'from delta import  # Delta Lake 라이브러리 임포트',
        r'DeltaTable': 'DeltaTable  # Delta 테이블 작업을 위한 클래스',
        r'configure_spark_with_delta_pip': 'configure_spark_with_delta_pip  # Delta Lake와 함께 Spark 설정',
        r'\.format\(["\']delta["\']\)': '.format("delta")  # Delta Lake 형식으로 저장',
        r'USING DELTA': 'USING DELTA  # Delta Lake 테이블 생성',
        r'\.saveAsTable\(': '.saveAsTable(  # 테이블로 저장',
        r'\.mode\(["\']overwrite["\']\)': '.mode("overwrite")  # 기존 데이터 덮어쓰기',
        r'\.mode\(["\']append["\']\)': '.mode("append")  # 기존 데이터에 추가',
        
        # 스트리밍 관련
        r'readStream': 'readStream  # 스트리밍 데이터 읽기',
        r'writeStream': 'writeStream  # 스트리밍 데이터 쓰기',
        r'\.trigger\(': '.trigger(  # 스트리밍 트리거 설정',
        r'\.outputMode\(': '.outputMode(  # 스트리밍 출력 모드 설정',
        r'\.start\(\)': '.start()  # 스트리밍 시작',
        r'\.awaitTermination\(\)': '.awaitTermination()  # 스트리밍 종료 대기',
        
        # 윈도우 함수 관련
        r'from pyspark\.sql\.window import Window': 'from pyspark.sql.window import Window  # 윈도우 함수를 위한 Window 클래스 임포트',
        r'Window\.partitionBy\(': 'Window.partitionBy(  # 윈도우 파티션 설정',
        r'Window\.orderBy\(': 'Window.orderBy(  # 윈도우 정렬 설정',
        r'\.over\(': '.over(  # 윈도우 함수 적용',
        r'row_number\(\)': 'row_number()  # 행 번호 생성',
        r'rank\(\)': 'rank()  # 순위 계산',
        r'dense_rank\(\)': 'dense_rank()  # 밀집 순위 계산',
        r'lag\(': 'lag(  # 이전 행 값 참조',
        r'lead\(': 'lead(  # 다음 행 값 참조',
        
        # 집계 함수 관련
        r'\.agg\(': '.agg(  # 집계 함수 적용',
        r'\.count\(\)': '.count()  # 행 개수 계산',
        r'\.sum\(': '.sum(  # 합계 계산',
        r'\.avg\(': '.avg(  # 평균 계산',
        r'\.max\(': '.max(  # 최댓값 계산',
        r'\.min\(': '.min(  # 최솟값 계산',
        r'collect_list\(': 'collect_list(  # 리스트로 수집',
        r'collect_set\(': 'collect_set(  # 중복 제거하여 집합으로 수집',
        
        # 조인 관련
        r'\.join\(': '.join(  # 데이터프레임 조인',
        r'\.crossJoin\(': '.crossJoin(  # 크로스 조인',
        r'inner': 'inner  # 내부 조인',
        r'left': 'left  # 왼쪽 조인',
        r'right': 'right  # 오른쪽 조인',
        r'outer': 'outer  # 외부 조인',
        
        # 데이터 타입 관련
        r'from pyspark\.sql\.types import': 'from pyspark.sql.types import  # Spark SQL 데이터 타입 임포트',
        r'StructType': 'StructType  # 구조체 타입',
        r'StructField': 'StructField  # 구조체 필드',
        r'StringType': 'StringType  # 문자열 타입',
        r'IntegerType': 'IntegerType  # 정수 타입',
        r'DoubleType': 'DoubleType  # 실수 타입',
        r'BooleanType': 'BooleanType  # 불린 타입',
        r'DateType': 'DateType  # 날짜 타입',
        r'TimestampType': 'TimestampType  # 타임스탬프 타입',
        r'ArrayType': 'ArrayType  # 배열 타입',
        
        # 캐싱 및 성능 관련
        r'\.cache\(\)': '.cache()  # 메모리에 캐싱',
        r'\.persist\(': '.persist(  # 스토리지 레벨 지정하여 캐싱',
        r'\.unpersist\(\)': '.unpersist()  # 캐시 해제',
        r'\.repartition\(': '.repartition(  # 파티션 재분배',
        r'\.coalesce\(': '.coalesce(  # 파티션 수 줄이기',
        
        # 널 처리 관련
        r'\.isNull\(\)': '.isNull()  # null 값 확인',
        r'\.isNotNull\(\)': '.isNotNull()  # null이 아닌 값 확인',
        r'\.fillna\(': '.fillna(  # null 값 채우기',
        r'\.dropna\(': '.dropna(  # null 값이 있는 행 제거',
        r'\.na\.fill\(': '.na.fill(  # null 값 채우기',
        r'\.na\.drop\(': '.na.drop(  # null 값이 있는 행 제거',
        
        # 문자열 함수 관련
        r'upper\(': 'upper(  # 대문자로 변환',
        r'lower\(': 'lower(  # 소문자로 변환',
        r'trim\(': 'trim(  # 공백 제거',
        r'substring\(': 'substring(  # 부분 문자열 추출',
        r'regexp_replace\(': 'regexp_replace(  # 정규식으로 문자열 치환',
        r'split\(': 'split(  # 문자열 분할',
        
        # 날짜/시간 함수 관련
        r'current_date\(\)': 'current_date()  # 현재 날짜',
        r'current_timestamp\(\)': 'current_timestamp()  # 현재 타임스탬프',
        r'date_format\(': 'date_format(  # 날짜 형식 변환',
        r'to_date\(': 'to_date(  # 문자열을 날짜로 변환',
        r'to_timestamp\(': 'to_timestamp(  # 문자열을 타임스탬프로 변환',
        r'year\(': 'year(  # 연도 추출',
        r'month\(': 'month(  # 월 추출',
        r'day\(': 'day(  # 일 추출',
        
        # 조건문 관련
        r'when\(': 'when(  # 조건문 시작',
        r'otherwise\(': 'otherwise(  # 기본값 설정',
        r'\.alias\(': '.alias(  # 컬럼 별칭 설정',
        
        # 파일 형식 관련
        r'\.option\(["\']inferSchema["\']\s*,\s*["\']true["\']\)': '.option("inferSchema", "true")  # 스키마 자동 추론',
        r'\.option\(["\']timestampFormat["\']\s*,': '.option("timestampFormat",  # 타임스탬프 형식 지정',
        r'\.option\(["\']dateFormat["\']\s*,': '.option("dateFormat",  # 날짜 형식 지정',
        
        # 기타 유용한 함수들
        r'\.distinct\(\)': '.distinct()  # 중복 제거',
        r'\.limit\(': '.limit(  # 결과 행 수 제한',
        r'\.sample\(': '.sample(  # 샘플링',
        r'\.randomSplit\(': '.randomSplit(  # 랜덤 분할',
        r'\.union\(': '.union(  # 데이터프레임 합치기',
        r'\.intersect\(': '.intersect(  # 교집합',
        r'\.except\(': '.except(  # 차집합',
        
        # 설정 관련
        r'spark\.conf\.set\(': 'spark.conf.set(  # Spark 설정 변경',
        r'spark\.sql\(': 'spark.sql(  # SQL 쿼리 실행',
        r'\.createOrReplaceTempView\(': '.createOrReplaceTempView(  # 임시 뷰 생성',
        r'\.createGlobalTempView\(': '.createGlobalTempView(  # 글로벌 임시 뷰 생성',
    }
    
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
        
        modified = False
        
        # 각 셀을 처리
        for cell in notebook.get('cells', []):
            if cell.get('cell_type') == 'code':
                source = cell.get('source', [])
                if isinstance(source, list):
                    # 각 라인에 대해 주석 추가
                    for i, line in enumerate(source):
                        original_line = line
                        
                        # 이미 한글 주석이 있는 라인은 건너뛰기
                        if re.search(r'[가-힣]', line):
                            continue
                            
                        # 주석 매핑 적용
                        for pattern, replacement in enhanced_mappings.items():
                            if re.search(pattern, line):
                                source[i] = re.sub(pattern, replacement, line)
                                if source[i] != original_line:
                                    modified = True
                                break
        
        # 수정된 경우에만 파일 저장
        if modified:
            with open(notebook_path, 'w', encoding='utf-8') as f:
                json.dump(notebook, f, ensure_ascii=False, indent=1)
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {notebook_path}: {e}")
        return False

def main():
    """메인 함수"""
    base_path = Path('/home/ec2-user/Data-Engineering-with-Databricks-Cookbook')
    
    # 모든 .ipynb 파일 찾기
    notebook_files = list(base_path.rglob('*.ipynb'))
    
    print(f"Found {len(notebook_files)} notebook files")
    
    success_count = 0
    modified_count = 0
    
    for notebook_file in notebook_files:
        print(f"Processing: {notebook_file}")
        result = enhance_korean_comments_in_notebook(notebook_file)
        if result is not False:
            success_count += 1
            if result is True:
                modified_count += 1
                print(f"  ✓ Enhanced with additional comments")
            else:
                print(f"  - No additional comments needed")
        else:
            print(f"  ✗ Failed to process")
    
    print(f"\nCompleted: {success_count}/{len(notebook_files)} files processed successfully")
    print(f"Enhanced: {modified_count} files with additional Korean comments")

if __name__ == "__main__":
    main()