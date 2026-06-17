defmodule Explorer.Repo.ShrunkInternalTransactions.Migrations.RemoveInternalTransactionsConstraints do
  use Ecto.Migration

  def change do
    # drop_if_exists handles the case where a later main-repo migration
    # (20250915135943_create_transaction_errors) has already dropped these
    # constraints — that happens when SHRINK_INTERNAL_TRANSACTIONS_ENABLED
    # is enabled after the main repo has already advanced past 20250915.
    drop_if_exists(constraint(:internal_transactions, :call_has_input, check: "type != 'call' OR input IS NOT NULL"))

    drop_if_exists(
      constraint(:internal_transactions, :call_has_error_or_result,
        check: """
        type != 'call' OR
        (gas IS NOT NULL AND
         ((error IS NULL AND gas_used IS NOT NULL AND output IS NOT NULL) OR
          (error IS NOT NULL AND output is NULL)))
        """
      )
    )
  end
end
