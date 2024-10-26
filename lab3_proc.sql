CREATE PROCEDURE lab3
	@YearsAgo int
AS
	SELECT 
		*
	FROM FactCurrencyRate FCR
		JOIN DimCurrency DC
			ON DC.CurrencyKey = FCR.CurrencyKey
	WHERE DC.CurrencyAlternateKey IN ('GBP', 'EUR')
		AND DATEDIFF(YEAR,FCR.Date, GETDATE()) = @YearsAgo
GO

EXEC lab3 @YearsAgo = 10