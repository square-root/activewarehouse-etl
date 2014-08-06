module ETL
  module Processor
    class AssertionSqlProcessor < ETL::Processor::SqlProcessor
      
      def process
        begin
          @logger.info 'Starting Assertions'
          super
          @logger.info "All Assertions Completed with status: '#{status}'"
        end
      end      

      def post_result_processor(file_name, result)
        unless result.first[0]==0
          @logger.error("Asserting Test Fails for #{file_name} value:(#{result.first[0]})")
          @status = 'Fail'
          ETL::Engine.exit_code = 1
        end
      end

      private
      def status
        @status || 'Success'
      end

    end
  end
end