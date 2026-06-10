package io.absurd.sdk;

public sealed interface TaskResultSnapshot {

    String state();

    record Pending() implements TaskResultSnapshot {
        @Override
        public String state() {
            return "pending";
        }
    }

    record Running() implements TaskResultSnapshot {
        @Override
        public String state() {
            return "running";
        }
    }

    record Sleeping() implements TaskResultSnapshot {
        @Override
        public String state() {
            return "sleeping";
        }
    }

    record Completed(JsonValue result) implements TaskResultSnapshot {
        @Override
        public String state() {
            return "completed";
        }
    }

    record Failed(JsonValue failure) implements TaskResultSnapshot {
        @Override
        public String state() {
            return "failed";
        }
    }

    record Cancelled() implements TaskResultSnapshot {
        @Override
        public String state() {
            return "cancelled";
        }
    }

    static boolean isTerminal(TaskResultSnapshot snapshot) {
        return snapshot instanceof Completed || snapshot instanceof Failed || snapshot instanceof Cancelled;
    }
}
