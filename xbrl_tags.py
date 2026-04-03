"""
================================================================================
XBRL TAG MAPPING - MERGED FROM ALL COMPANIES
================================================================================
FDA-4: Comparative Financial Analysis Pipeline
This file contains all possible XBRL tags for each financial field.

Usage:
    from xbrl_tags import FEATURE_MAP
================================================================================
"""

FEATURE_MAP = {

    # ================= INCOME STATEMENT =================

    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueServicesGross",
        "SalesRevenueServicesNet",
        "SalesRevenueGoodsNet",
        "SubscriptionRevenue",
        "TechnologyServicesRevenue",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "RevenueFromRelatedParties",
        "RevenueNotFromContractWithCustomer",
        "SalesTypeLeaseRevenue",
        "FinancialServicesRevenue",
        "SegmentReportingInformationRevenueFromExternalCustomers",
        "AdvertisingRevenue",
        "OtherSalesRevenueNet",
        "LicensesRevenue",
        "LicenseAndMaintenanceRevenue",
        "MaintenanceRevenue",
        "LicenseAndServicesRevenue",
        "InterestAndFeeIncomeLoansAndLeases",
        "SalesRevenueEnergyServices",
    ],

    "net_income": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "NetIncomeLossAttributableToParentDiluted",
        "NetIncomeLossAvailableToCommonStockholdersDiluted",
        "IncomeLossAttributableToParent",
        "NetIncomeLossIncludingPortionAttributableToNonredeemableNoncontrollingInterest",
        "NetIncomeLossAttributableToNoncontrollingInterest",
    ],

    "gross_profit": [
        "GrossProfit",
    ],

    "operating_income": [
        "OperatingIncomeLoss",
        "SegmentReportingInformationOperatingIncomeLoss",
        "SegmentReportingSegmentOperatingProfitLoss",
        "DisposalGroupIncludingDiscontinuedOperationOperatingIncomeLoss",
    ],

    "cost_of_revenue": [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsSold",
        "CostOfServices",
        "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization",
        "OtherCostOfServices",
        "CostOfGoodsAndServicesSoldDepreciationAndAmortization",
        "CostOfGoodsSoldSalesTypeLease",
        "CostOfGoodsAndServicesSoldAmortization",
        "CostOfServicesAmortization",
        "CostOfGoodsSoldSubscription",
        "TechnologyServicesCosts",
        "CostOfGoodsSoldAmortization",
        "CostOfServicesMaintenanceCosts",
        "LicenseCosts",
        "OperatingExpenses",
        "OtherCostAndExpenseOperating",
        "CostsAndExpenses",
        "CostOfServicesEnergyServices",
    ],

    "interest_expense": [
        "InterestExpense",
        "InterestExpenseDebt",
        "InterestIncomeExpenseNonoperatingNet",
        "InterestCostsIncurred",
        "InterestExpenseNonoperating",
        "InterestIncomeExpenseNet",
        "InterestExpenseDebtExcludingAmortization",
        "InterestRevenueExpenseNet",
        "InterestPaidNet",
        "InterestExpenseLongTermDebt",
        "InterestPaid",
        "FinanceLeaseInterestExpense",
        "InterestIncomeOther",
        "InterestOnConvertibleDebtNetOfTax",
        "InterestCostsCapitalized",
    ],

    "eps_basic": [
    "EarningsPerShareBasic",
    "EarningsPerShareBasicAndDiluted",
    "IncomeLossFromContinuingOperationsPerBasicShare",
],

"eps_diluted": [
    "EarningsPerShareDiluted",
    "IncomeLossFromContinuingOperationsPerDilutedShare",
    "EarningsPerShareBasicAndDiluted",

],

    # ================= BALANCE SHEET =================

    "total_assets": [
        "Assets",
        "NoncurrentAssets",
        "AssetsNoncurrent",
        "AssetsOfDisposalGroupIncludingDiscontinuedOperationCurrent",
    ],

    "total_liabilities": [
        "Liabilities",
        "LiabilitiesNoncurrent",
        "AccountsPayableCurrentAndNoncurrent",
    ],

    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquityOther",
        "RetainedEarningsAccumulatedDeficit",
        "CommonStocksIncludingAdditionalPaidInCapital",
        "AdditionalPaidInCapital",
        "CommonStockIncludingAdditionalPaidInCapital",
        "AdditionalPaidInCapitalCommonStock",
        "NonredeemableNoncontrollingInterest",
    ],

    "current_assets": [
        "AssetsCurrent",
        "OtherAssetsCurrent",
        "PrepaidExpenseAndOtherAssetsCurrent",
        "OtherAssetsMiscellaneousCurrent",
        "DepositsAssetsCurrent",
        "ContractWithCustomerAssetNetCurrent",
    ],

    "current_liabilities": [
        "LiabilitiesCurrent",
        "AccruedLiabilitiesCurrent",
        "AccountsPayableCurrent",
        "OtherLiabilitiesCurrent",
        "EmployeeRelatedLiabilitiesCurrent",
        "AccruedLiabilitiesAndOtherLiabilities",
        "OtherAccruedLiabilitiesCurrent",
        "TaxesPayableCurrent",
        "AccountsPayableAndAccruedLiabilitiesCurrent",
        "PayablesToCustomers",
        "SettlementLiabilitiesCurrent",
        "CustomerDepositsCurrent",
        "CustomerAdvancesCurrent",
        "ContractWithCustomerLiabilityCurrent",
        "ContractWithCustomerRefundLiabilityCurrent",
        "AccruedEmployeeBenefitsCurrent",
        "AccruedMarketingCostsCurrent",
        "AccruedProfessionalFeesCurrent",
        "DeferredRevenueCurrent",
    ],

    "cash_and_equivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "Cash",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "CashEquivalentsAtCarryingValue",
        "RestrictedCashAndCashEquivalents",
        "RestrictedCashAndCashEquivalentsAtCarryingValue",
        "RestrictedCash",
        "RestrictedCashCurrent",
        "RestrictedCashNoncurrent",
        "CashAndCashEquivalentsPeriodIncreaseDecrease",
        "MoneyMarketFundsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
    ],

    "short_term_investments": [
        "ShortTermInvestments",
        "MarketableSecuritiesCurrent",
        "AvailableForSaleSecuritiesCurrent",
        "CashCashEquivalentsAndShortTermInvestments",
        "AvailableForSaleSecurities",
        "MarketableSecurities",
        "ShortTermLeaseCost",
        "TimeDeposits",
        "AvailableForSaleSecuritiesDebtSecuritiesCurrent",
        "TradingSecuritiesCurrent",
        "TradingSecurities",
        "AvailableForSaleSecuritiesDebtSecurities",
        "DebtSecuritiesAvailableForSaleExcludingAccruedInterestCurrent",
        "EquitySecuritiesFvNi",
        "EquitySecuritiesWithoutReadilyDeterminableFairValueAmount",
        "HeldToMaturitySecurities",
        "HeldToMaturitySecuritiesFairValue",
        "Investments",
        "TimeDepositsAtCarryingValue",
        "LongTermInvestments",
        "CertificatesOfDepositAtCarryingValue",
        "AvailableForSaleSecuritiesDebtSecuritiesNoncurrent",
        "OtherLongTermInvestments",
        "EquitySecuritiesFvNiCurrentAndNoncurrent",
        "CryptoAssetFairValue",
        "AvailableForSaleSecuritiesAmortizedCost",
        "RestrictedCashAndInvestmentsCurrent",
    ],

    "accounts_receivable": [
        "AccountsReceivableNetCurrent",
        "AccountsReceivableGrossCurrent",
        "NontradeReceivablesCurrent",
        "UnbilledContractsReceivable",
        "UnbilledReceivablesCurrent",
        "AccountsReceivableRelatedParties",
        "AllowanceForDoubtfulAccountsReceivable",
        "AllowanceForDoubtfulAccountsReceivableCurrent",
        "AccountsNotesAndLoansReceivableNetCurrent",
        "NotesAndLoansReceivableNetCurrent",
        "AccountsReceivableSale",
        "FinancingReceivableExcludingAccruedInterestAfterAllowanceForCreditLossCurrent",
        "AccountsReceivableNet",
        "AccountsReceivableNetNoncurrent",
        "OtherReceivablesGrossCurrent",
        "OtherReceivablesNetCurrent",
        "TradeReceivablesHeldForSaleAmount",
        "ReceivablesNetCurrent",
        "OtherReceivables",
        "ProvisionForDoubtfulAccounts",
        "AccountsAndOtherReceivablesNetCurrent",
        "AccountsReceivableBilledForLongTermContractsOrPrograms",
        "ContractsReceivable",
        "FinancingReceivableAccruedInterestBeforeAllowanceForCreditLoss",
        "LoansReceivableHeldForSaleAmount",
        "AllowanceForNotesAndLoansReceivableCurrent",
        "LoansReceivableHeldForSaleNetNotPartOfDisposalGroup",
        "FinancingReceivableSignificantSales",
        "AccountsAndNotesReceivableNet",
        "NotesReceivableNet",
    ],

    "long_term_debt": [
        "LongTermDebtNoncurrent",
        "LongTermDebt",
        "LongTermDebtCurrent",
        "DebtLongtermAndShorttermCombinedAmount",
        "DebtAndCapitalLeaseObligations",
        "LongTermDebtAndCapitalLeaseObligations",
        "OtherLongTermDebt",
        "ConvertibleLongTermNotesPayable",
        "ConvertibleNotesPayable",
        "SeniorNotes",
        "DebtCurrent",
        "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",
        "CommercialPaper",
        "ShortTermBorrowings",
        "SeniorLongTermNotes",
        "LineOfCreditFacilityAmountOutstanding",
        "NotesPayableCurrent",
        "LongTermNotesPayable",
        "ConvertibleDebtCurrent",
        "NotesPayable",
        "OtherLongTermDebtNoncurrent",
        "OtherLongTermDebtCurrent",
        "ConvertibleNotesPayableCurrent",
        "ConvertibleDebt",
        "ConvertibleDebtNoncurrent",
        "CapitalLeaseObligations",
        "CapitalLeaseObligationsCurrent",
        "CapitalLeaseObligationsNoncurrent",
        "LinesOfCreditCurrent",
        "LongTermLineOfCredit",
        "LongTermLoansPayable",
    ],

    "inventory": [
        "InventoryNet",
        "InventoryFinishedGoodsNetOfReserves",
        "InventoryPartsAndComponentsNetOfReserves",
        "InventoryFinishedGoods",
        "InventoryRawMaterials",
        "InventoryWorkInProcess",
        "InventoryRawMaterialsNetOfReserves",
        "InventoryWorkInProcessNetOfReserves",
        "InventoryWorkInProcessAndRawMaterialsNetOfReserves",
    ],
}


# Total unique tags per field (for reference)
TAG_COUNTS = {field: len(tags) for field, tags in FEATURE_MAP.items()}

# Print summary when run directly
if __name__ == "__main__":
    print("=" * 60)
    print("XBRL TAG MAPPING SUMMARY")
    print("=" * 60)
    total_tags = sum(len(tags) for tags in FEATURE_MAP.values())
    print(f"Total fields: {len(FEATURE_MAP)}")
    print(f"Total unique tags: {total_tags}")
    print()
    print("Tags per field:")
    for field, count in TAG_COUNTS.items():
        print(f"  • {field}: {count} tags")
