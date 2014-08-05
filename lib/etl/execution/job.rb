module ETL #:nodoc:
  module Execution #:nodoc:
    # Persistent class representing an ETL job
    class Job < Base
      belongs_to :batch
      # file_name column to stores source file
      attr_accessible :control_file, :file_name, :status, :batch_id
    end
  end
end
