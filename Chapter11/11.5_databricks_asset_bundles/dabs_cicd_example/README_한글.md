# dabs_cicd_example

'dabs_cicd_example' 프로젝트는 default-python 템플릿을 사용하여 생성되었습니다.

## 시작하기

1. https://docs.databricks.com/dev-tools/cli/databricks-cli.html 에서 Databricks CLI를 설치하세요

2. Databricks 워크스페이스에 인증하세요:
    ```
    $ databricks configure
    ```

3. 이 프로젝트의 개발 사본을 배포하려면 다음을 입력하세요:
    ```
    $ databricks bundle deploy --target dev
    ```
    (참고: "dev"는 기본 대상이므로 여기서 `--target` 매개변수는 선택사항입니다.)

    이것은 이 프로젝트에 정의된 모든 것을 배포합니다.
    예를 들어, 기본 템플릿은 `[dev yourname] dabs_cicd_example_job`이라는 작업을
    워크스페이스에 배포합니다.
    워크스페이스를 열고 **Workflows**를 클릭하여 해당 작업을 찾을 수 있습니다.

4. 마찬가지로 프로덕션 사본을 배포하려면 다음을 입력하세요:
   ```
   $ databricks bundle deploy --target prod
   ```

5. 작업이나 파이프라인을 실행하려면 "run" 명령을 사용하세요:
   ```
   $ databricks bundle run
   ```

6. 선택적으로, Visual Studio Code용 Databricks 확장과 같은 개발자 도구를 설치하세요:
   https://docs.databricks.com/dev-tools/vscode-ext.html

7. 이 프로젝트에 사용된 Databricks 자산 번들 형식 및 CI/CD 구성에 대한 문서는
   https://docs.databricks.com/dev-tools/bundles/index.html 을 참조하세요.