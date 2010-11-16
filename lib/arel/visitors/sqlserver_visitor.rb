module Arel
  # module Nodes
  #   class SelectStatement
  #     def complex_count_sql?
  #       projections = self.projections
  #       projections.first.is_a?(Arel::Count) && projections.size == 1 &&
  #         (self.offset.present? || self.wheres.present?) && self.joins(self).blank?
  #     end
  # 
  # 
  #     def eager_limiting_select?
  #       self.single_distinct_select? && self.limit_only? && self.group_clauses.blank?
  #     end
  # 
  #     def limit_only?
  #       self.limit.present? && self.offset.blank?
  #     end
  #     
  #     def limit_clause
  #       "TOP (#{self.limit.to_i}) "
  #     end
  #     
  #   end
  # end

  class SelectManager
    def lock locking = true
      # FIXME: do we even need to store this?  If locking is +false+ shouldn't
      # we just remove the node from the AST?
      @head.lock = Nodes::Lock.new(locking)
      self
    end
  end

  module Nodes
    class Lock
      attr_accessor :value

      def initialize value = true
        @value = value
      end
    end
  end
  
  module Visitors
    class SQLServer < Arel::Visitors::ToSql
      private

      def orig_visit_Arel_Nodes_SelectStatement o
        [
          o.cores.map { |x| visit_Arel_Nodes_SelectCore x, o.lock }.join,
          ("ORDER BY #{o.orders.map { |x| visit x }.join(', ')}" unless o.orders.empty?),
          ("LIMIT #{o.limit}" if o.limit),
          (visit(o.offset) if o.offset),
        ].compact.join ' '
      end

      def visit_Arel_Nodes_SelectCore(o, lock = nil)
        [
          "SELECT #{o.projections.map { |x| visit x }.join ', '}",
          ("FROM #{visit o.froms}" if o.froms),
          (visit(lock) if lock),
          ("WHERE #{o.wheres.map { |x| visit x }.join ' AND ' }" unless o.wheres.empty?),
          ("GROUP BY #{o.groups.map { |x| visit x }.join ', ' }" unless o.groups.empty?),
          (visit(o.having) if o.having),
        ].compact.join ' '
      end
      
      def visit_Arel_Nodes_SelectStatement(o)
        # o.joins = correlated_safe_joins(o)
        if o.limit && o.offset
          o        = o.dup
          limit    = o.limit.to_i
          offset   = o.offset
          o.limit  = nil
          o.offset = nil
          orders   = o.orders
          o.orders = []
          sql = orig_visit_Arel_Nodes_SelectStatement(o)
          distinct = !!(sql =~ /DISTINCT/i)
          sql = <<-ENDOFSQL
          SELECT #{distinct ? 'DISTINCT ' : ''}TOP (#{limit}) [__rnt].*
          FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY #{unique_orders(rowtable_order_clauses(o, orders)).join(', ')}) AS [__rn],
            #{sql.sub(/(SELECT(?:\sDISTINCT)?) /i, '')}
          ) AS [__rnt]
          WHERE [__rnt].[__rn] > #{visit offset}
          ENDOFSQL
          sql
        elsif o.limit
          o       = o.dup
          limit   = o.limit.to_i
          o.limit = nil
          sql = orig_visit_Arel_Nodes_SelectStatement(o)
          sql.sub(/(SELECT(?:\sDISTINCT)?) /i, "\\1 TOP (#{limit}) ")
        elsif o.offset
          o        = o.dup
          offset   = o.offset
          o.offset = nil
          orders   = o.orders
          o.orders = []
          sql = orig_visit_Arel_Nodes_SelectStatement(o)
          sql = <<-ENDOFSQL
          SELECT *
          FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY #{unique_orders(rowtable_order_clauses(o, orders)).join(', ')}) AS [__rn],
            #{sql}
          ) AS [__rnt]
          WHERE [__rnt].[__rn] > #{visit offset}
          ENDOFSQL
          puts sql
          sql
        else
          super
        end
      end
      
      def select_sql_with_complex_count
        joins   = correlated_safe_joins
        wheres  = relation.where_clauses
        groups  = relation.group_clauses
        havings = relation.having_clauses
        orders  = relation.order_clauses
        taken   = relation.taken.to_i
        skipped = relation.skipped.to_i
        top_clause = "TOP (#{taken+skipped}) " if relation.taken.present?
        build_query \
          "SELECT COUNT([count]) AS [count_id]",
          "FROM (",
            "SELECT #{top_clause}ROW_NUMBER() OVER (ORDER BY #{unique_orders(rowtable_order_clauses).join(', ')}) AS [__rn],",
            "1 AS [count]",
            "FROM #{relation.from_clauses}",
            (locked unless locked.blank?),
            (joins unless joins.blank?),
            ("WHERE #{wheres.join(' AND ')}" unless wheres.blank?),
            ("GROUP BY #{groups.join(', ')}" unless groups.blank?),
            ("HAVING #{havings.join(' AND ')}" unless havings.blank?),
            ("ORDER BY #{unique_orders(orders).join(', ')}" unless orders.blank?),
          ") AS [__rnt]",
          "WHERE [__rnt].[__rn] > #{relation.skipped.to_i}"
      end
      
      def select_sql_without_skipped(relation, windowed=false)
        selects = relation.select_clauses
        joins   = correlated_safe_joins
        wheres  = relation.where_clauses
        groups  = relation.group_clauses
        havings = relation.having_clauses
        orders  = relation.order_clauses
        if windowed
          selects = expression_select? ? selects : selects.map{ |sc| clause_without_expression(sc) }          
        elsif eager_limiting_select?
          groups = selects.map { |sc| clause_without_expression(sc) }
          selects = selects.map { |sc| "#{taken_clause}#{clause_without_expression(sc)}" }
          orders = orders.map do |oc|
            oc.split(',').reject(&:blank?).map do |c|
              max = c =~ /desc\s*/i
              c = clause_without_expression(c).sub(/(asc|desc)/i,'').strip
              max ? "MAX(#{c})" : "MIN(#{c})"
            end.join(', ')
          end
        elsif taken_only?
          fsc = "#{taken_clause}#{selects.first}"
          selects = selects.tap { |sc| sc.shift ; sc.unshift(fsc) }
        end
        build_query(
          (windowed ? selects.join(', ') : "SELECT #{selects.join(', ')}"),
          "FROM #{relation.from_clauses}",
          (locked unless locked.blank?),
          (joins unless joins.blank?),
          ("WHERE #{wheres.join(' AND ')}" unless wheres.blank?),
          ("GROUP BY #{groups.join(', ')}" unless groups.blank?),
          ("HAVING #{havings.join(' AND ')}" unless havings.blank?),
          ("ORDER BY #{unique_orders(orders).join(', ')}" if orders.present? && !windowed))
      end
      
      def select_sql_with_skipped(relation)
        tc = taken_clause(relation) if relation.limit && !single_distinct_select?
        build_query \
          "SELECT #{tc}#{rowtable_select_clauses.join(', ')}",
          "FROM (",
            "SELECT ROW_NUMBER() OVER (ORDER BY #{unique_orders(rowtable_order_clauses).join(', ')}) AS [__rn],",
            select_sql_without_skipped(relation, true),
          ") AS [__rnt]",
          "WHERE [__rnt].[__rn] > #{visit relation.offset}"
      end

      def taken_clause(relation)
        "TOP (#{relation.limit.to_i}) "
      end

      def visit_Bignum o; o end

      def visit_Arel_Nodes_Offset o
        visit o.value.to_i
      end
      
      def visit_Arel_Nodes_Lock o
        o.value == true ? "WITH(HOLDLOCK, ROWLOCK)" : o.value
      end
      
      def unique_orders(orders)
        return [] if orders.nil?
        existing_columns = {}
        orders.inject([]) do |queued_orders, order|
          table_column, dir = clause_without_expression(order).split
          table_column = table_column.tr('[]','').split('.')
          table, column = table_column.size == 2 ? table_column : table_column.unshift('')
          existing_columns[table] ||= []
          unless existing_columns[table].include?(column)
            existing_columns[table] << column
            queued_orders << order 
          end
          queued_orders
        end
      end
      
      def rowtable_order_clauses(o, orders)
        orders = orders.map { |x| visit x }.join(', ')
        if !orders.empty?
          orders
        elsif o.cores.first.froms.is_a?(Nodes::Join)
          table_names_from_select_clauses(o).map { |tn| quote_column_name("#{tn}.#{pk_for_table(tn)}") }
        elsif o.cores.first.froms
          [quote_column_name "#{o.cores.first.froms.name}.#{o.cores.first.froms.primary_key.name}"]
        end
      end
      
      def clause_without_expression(clause)
        clause.to_s.split(',').map do |c|
          c.strip!
          c.sub!(/^(COUNT|SUM|MAX|MIN|AVG)\s*(\((.*)\))?/,'\3')
          c.sub!(/^DISTINCT\s*/,'')
          c.sub!(/TOP\s*\(\d+\)\s*/i,'')
          c.strip
        end.join(', ')
      end
      
      def correlated_safe_joins(o)
        joins = o.joins(self)
        if joins.present?
          find_and_fix_uncorrelated_joins(o)
          relation.joins(self)
        else
          joins
        end
      end
      
      def find_and_fix_uncorrelated_joins(o)
        join_relation = o.relation
        while join_relation.present?
          return join_relation if uncorrelated_inner_join_relation?(join_relation)
          join_relation = join_relation.relation rescue nil
        end
      end

      def uncorrelated_inner_join_relation?(r)
        if r.is_a?(Arel::StringJoin) && r.relation1.is_a?(Arel::OuterJoin) && 
            r.relation2.is_a?(String) && r.relation2.starts_with?('INNER JOIN')
          outter_join_table1 = r.relation1.relation1.table
          outter_join_table2 = r.relation1.relation2.table
          string_join_table_info = r.relation2.split(' ON ').first.sub('INNER JOIN ','')
          return nil if string_join_table_info.include?(' AS ') # Assume someone did something right.
          string_join_table_name = unqualify_table_name(string_join_table_info)
          uncorrelated_table1 = string_join_table_name == outter_join_table1.name && string_join_table_name == outter_join_table1.alias.name
          uncorrelated_table2 = string_join_table_name == outter_join_table2.name && string_join_table_name == outter_join_table2.alias.name
          if uncorrelated_table1 || uncorrelated_table2
            on_index = r.relation2.index(' ON ')
            r.relation2.insert on_index, " AS [#{string_join_table_name}_crltd]"
            r.relation2.sub! "[#{string_join_table_name}].", "[#{string_join_table_name}_crltd]."
            return r
          else
            return nil
          end
        end
      rescue
        nil
      end
      
      def table_name_from_select_clause(sc)
        parts = clause_without_expression(sc).split('.')
        tn = parts.third ? parts.second : (parts.second ? parts.first : nil)
        tn ? tn.tr('[]','') : nil
      end
      
      def table_names_from_select_clauses(o)
        o.cores.map do |c|
          c.projections.map { |p| p.split(',') }.flatten.map { |sc| table_name_from_select_clause(sc) }
        end.flatten.compact.uniq
      end

      def pk_for_table(table_name)
        @engine.connection.primary_key(table_name)
      end
      
    end
  end
end

Arel::Visitors::VISITORS['sqlserver'] = Arel::Visitors::SQLServer