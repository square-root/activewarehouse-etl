module ETL
  module Processor
    class TransactionalSqlProcessor < ETL::Processor::SqlProcessor
      
      ## 
      # Performs SqlProcessor task in transaction
      # source:        Collects empty hash, appends list of sql queires SqlProcessor executes
      # source_type:   Filter for hwat is send on source, sql path || sql query
      # logger:        For log results and output
      # db_connection: Database connection to be used
      # show_counts:   No of records inserted, deleted or updated
      ##
      def process
        begin
          @logger.info 'Starting Db Transaction'
          @db_connection.begin_db_transaction()
          super
          @logger.info 'Commiting Transaction'
          @db_connection.commit_db_transaction()
        rescue => e
          @logger.error 'Rolling Back db Transaction'
          @db_connection.rollback_db_transaction()
        end
      end      
    end
  end
end