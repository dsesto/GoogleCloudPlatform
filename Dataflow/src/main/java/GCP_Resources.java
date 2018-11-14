// Helper class in order to define the project-specific GCP resources

class GCP_Resources {
    private String subscription;
    private String bq_input;
    private String bq_output_good;
    private String bq_output_bad;

    GCP_Resources() {
        subscription = "projects/[PROJECT_ID]/subscriptions/[SUBSCRIPTION_NAME]";
        bq_input = "[PROJECT_ID]:[DATASET].[INPUT_TABLE]";
        bq_output_good = "[PROJECT_ID]:[DATASET].[OUTPUT_GOOD_TABLE]";
        bq_output_bad = "[PROJECT_ID]:[DATASET].[OUTPUT_BAD_TABLE]";
    }

    String getSubscription() {
        return subscription;
    }

    String getBq_input() {
        return bq_input;
    }

    String getBq_output_good() {
        return bq_output_good;
    }

    String getBq_output_bad() {
        return bq_output_bad;
    }
}
