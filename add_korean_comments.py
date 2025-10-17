#!/usr/bin/env python3
"""
Databricks 노트북 파일들에 한글 주석을 추가하는 스크립트
"""

import os
import json
import re
from pathlib import Path

def add_korean_comments_to_notebook(notebook_path):
    """노트북 파일에 한글 주석을 추가하는 함수"""
    
    # 공통 주석 매핑
    comment_mappings = {
        # SparkSession 관련
        r'from pyspark\.sql import SparkSession': 'from pyspark.sql import SparkSession  # Spark SQL 작업을 위한 SparkSession 임포트',
        r'SparkSession\.builder': 'SparkSession.builder  # SparkSession 빌더 패턴 시작',
        r'\.appName\(["\']([^"\']+)["\']\)': r'.appName("\1")  # 애플리케이션 이름 설정',
        r'\.master\(["\']([^"\']+)["\']\)': r'.master("\1")  # Spark 마스터 URL 설정',
        r'\.config\(["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']\)': r'.config("\1", "\2")  # Spark 설정 옵션',
        r'\.getOrCreate\(\)': '.getOrCreate()  # SparkSession 생성 또는 기존 세션 반환',
        r'spark\.sparkContext\.setLogLevel\(["\']ERROR["\']\)': 'spark.sparkContext.setLogLevel("ERROR")  # 로그 레벨을 ERROR로 설정',
        
        # 데이터 읽기 관련
        r'spark\.read\.format\(["\']csv["\']\)': 'spark.read.format("csv")  # CSV 형식으로 데이터 읽기',
        r'spark\.read\.format\(["\']json["\']\)': 'spark.read.format("json")  # JSON 형식으로 데이터 읽기',
        r'spark\.read\.format\(["\']parquet["\']\)': 'spark.read.format("parquet")  # Parquet 형식으로 데이터 읽기',
        r'\.option\(["\']header["\']\s*,\s*["\']true["\']\)': '.option("header", "true")  # 첫 번째 행을 헤더로 사용',
        r'\.option\(["\']multiLine["\']\s*,\s*["\']true["\']\)': '.option("multiLine", "true")  # 여러 줄 JSON 처리',
        r'\.load\(': '.load(  # 파일 로드',
        
        # DataFrame 작업 관련
        r'\.show\(\)': '.show()  # DataFrame 내용 출력',
        r'\.printSchema\(\)': '.printSchema()  # DataFrame 스키마 구조 출력',
        r'\.select\(': '.select(  # 컬럼 선택',
        r'\.filter\(': '.filter(  # 데이터 필터링',
        r'\.where\(': '.where(  # 조건부 필터링',
        r'\.groupBy\(': '.groupBy(  # 그룹화',
        r'\.orderBy\(': '.orderBy(  # 정렬',
        r'\.sort\(': '.sort(  # 정렬 (orderBy와 동일)',
        r'\.withColumn\(': '.withColumn(  # 새 컬럼 추가 또는 기존 컬럼 수정',
        r'\.withColumnRenamed\(': '.withColumnRenamed(  # 컬럼 이름 변경',
        r'\.dropDuplicates\(': '.dropDuplicates(  # 중복 데이터 제거',
        r'\.drop\(': '.drop(  # 컬럼 삭제',
        
        # 함수 관련
        r'from pyspark\.sql\.functions import': 'from pyspark.sql.functions import  # Spark SQL 함수들 임포트',
        r'explode\(': 'explode(  # 배열을 개별 행으로 분해',
        r'col\(': 'col(  # 컬럼 참조',
        r'lit\(': 'lit(  # 리터럴 값',
        r'concat\(': 'concat(  # 문자열 연결',
        r'transform\(': 'transform(  # 배열 변환',
        
        # UDF 관련
        r'from pyspark\.sql\.functions import udf': 'from pyspark.sql.functions import udf  # 사용자 정의 함수 임포트',
        r'udf\(': 'udf(  # 사용자 정의 함수 생성',
        r'spark\.udf\.register\(': 'spark.udf.register(  # UDF를 SQL에서 사용할 수 있도록 등록',
        
        # 세션 종료
        r'spark\.stop\(\)': 'spark.stop()  # Spark 세션 종료 - 리소스 정리',
        
        # 일반적인 주석
        r'# Create': '# 생성',
        r'# Read': '# 읽기',
        r'# Display': '# 출력',
        r'# Show': '# 표시',
        r'# Print': '# 출력',
        r'# Apply': '# 적용',
        r'# Filter': '# 필터링',
        r'# Sort': '# 정렬',
        r'# Group': '# 그룹화',
        r'# Transform': '# 변환',
        r'# Load': '# 로드',
        r'# Save': '# 저장',
        r'# Write': '# 쓰기',
    }
    
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
        
        # 각 셀을 처리
        for cell in notebook.get('cells', []):
            if cell.get('cell_type') == 'code':
                source = cell.get('source', [])
                if isinstance(source, list):
                    # 각 라인에 대해 주석 추가
                    for i, line in enumerate(source):
                        # 이미 한글 주석이 있는지 확인
                        if re.search(r'[가-힣]', line):
                            continue
                            
                        # 주석 매핑 적용
                        for pattern, replacement in comment_mappings.items():
                            if re.search(pattern, line):
                                source[i] = re.sub(pattern, replacement, line)
                                break
        
        # 수정된 노트북 저장
        with open(notebook_path, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, ensure_ascii=False, indent=1)
        
        return True
        
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
    for notebook_file in notebook_files:
        print(f"Processing: {notebook_file}")
        if add_korean_comments_to_notebook(notebook_file):
            success_count += 1
            print(f"  ✓ Successfully processed")
        else:
            print(f"  ✗ Failed to process")
    
    print(f"\nCompleted: {success_count}/{len(notebook_files)} files processed successfully")

if __name__ == "__main__":
    main()