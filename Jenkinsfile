pipeline {
    agent any
    stages {
        stage('Test') {
            steps {
                bat label: 'Run BDD', script: 'C:\\Users\\c_mow\\AppData\\Local\\Programs\\Python\\Python37\\Scripts\\behave.exe --junit'
                junit 'reports/*.xml'
            }
        }
    }
}