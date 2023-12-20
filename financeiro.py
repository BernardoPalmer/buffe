from sqlalchemy import create_engine
import pandas as pd
conn_str = (
    "mssql+pyodbc://Sa:P%40ssw0rd2023%40%23%24@"
    "192.168.1.43/SOUTTOMAYOR?"
    "driver=ODBC%20Driver%2011%20for%20SQL%20Server"
)
engine = create_engine(conn_str, pool_pre_ping=True)
from datetime import datetime, timedelta

# Get the current date in YYYY-MM-DD format
current_date = datetime.now()

# Adding one month to the current date
# Since Python's datetime module doesn't directly support adding months,
# a workaround is to add 30 days as an approximation for one month.
one_month_later = current_date + timedelta(days=30)

# Formatting the date
current_date2 = one_month_later.strftime('%Y-%m-%d')
current_date = current_date.strftime('%Y-%m-%d')

query = f"""
SELECT PK_DOCTOPED,TOTALDOCTO, DTEVENTO
FROM TPADOCTOPED
WHERE TPDOCTO IN ('EC','OR')
AND SITUACAO IN ('B','V')
--AND DTEVENTO >= '2012-01-30 00:00:00'
--AND DTEVENTO <= '2023-12-31 00:00:00'
AND DTEVENTO > '2023-12-31 00:00:00'
AND DTEVENTO <= '{current_date}'
"""

RECEITA = pd.read_sql(query, engine)

query = f"""
SELECT --A.PK_DESPESA,
       --A.NATUREZA,
       A.IDX_OPFINANCEIRA,
       B.RDX_DESPESA,
       B.VALORAPAGAR,
	   B.DTINC,
	   B.DTEMISSAO,
       B.DTPAGTO
FROM TPADESPESA A
JOIN TPADESPESAPARC B ON A.PK_DESPESA = B.RDX_DESPESA
JOIN TPAOPFINANCEIRA ON A.IDX_OPFINANCEIRA = TPAOPFINANCEIRA.PK_OPFINANCEIRA

--WHERE DTEMISSAO >= '2012-01-30 00:00:00'
--AND DTEMISSAO <= '2023-12-31 00:00:00'
WHERE DTEMISSAO > '2023-12-31 00:00:00'
AND DTEMISSAO <= '{current_date2}'
AND TPAOPFINANCEIRA.DESCRICAO != 'Eventos sociais'
AND TPAOPFINANCEIRA.DESCRICAO != 'Eventos corporativos'
AND TPAOPFINANCEIRA.DESCRICAO != 'Encomendas'
AND B.SITUACAO != 'C'
"""

DESPESA = pd.read_sql(query, engine)

RECEITA2 = pd.read_csv('RECEITA.csv')
DESPESA2 = pd.read_csv('DESPESA.csv')
OPFIN = pd.read_csv('OPFIN.csv')

RECEITA = pd.concat([RECEITA, RECEITA2], ignore_index=True)
DESPESA = pd.concat([DESPESA, DESPESA2], ignore_index=True)

# Convert 'DTEVENTO' to datetime format
#RECEITA['DTEVENTO'] = pd.to_datetime(RECEITA['DTEVENTO'])
# Convert 'DTEVENTO' to datetime
RECEITA['DTEVENTO'] = pd.to_datetime(RECEITA['DTEVENTO'], format='%Y-%m-%d %H:%M:%S.%f')

# Set 'DTEVENTO' as the index
RECEITA.set_index('DTEVENTO', inplace=True)

# Group by month and sum 'TOTALDOCTO'
result = RECEITA.resample('M').sum()['TOTALDOCTO']

DESPESA['DTEMISSAO'] = pd.to_datetime(DESPESA['DTEMISSAO'], format='%Y-%m-%d %H:%M:%S.%f')
DESPESA['DTPAGTO'] = pd.to_datetime(DESPESA['DTPAGTO'], format='%Y-%m-%d %H:%M:%S.%f')
DESPESA['DTINC'] = pd.to_datetime(DESPESA['DTINC'], format='%Y-%m-%d %H:%M:%S.%f')

ops = DESPESA['IDX_OPFINANCEIRA'].unique()
OPFIN['PK_OPFINANCEIRA'] = OPFIN['PK_OPFINANCEIRA'].apply(lambda x: int(str(x).replace(" ", "")))
filtered_despesa_df = DESPESA.copy()

def can_be_int(value):
    try:
        int(str(value).replace(" ", ""))
        return True
    except ValueError:
        return False

filtered_despesa_df = filtered_despesa_df[filtered_despesa_df['IDX_OPFINANCEIRA'].apply(can_be_int)]

# Perform the conversion to int
filtered_despesa_df['IDX_OPFINANCEIRA'] = filtered_despesa_df['IDX_OPFINANCEIRA'].apply(lambda x: int(str(x).replace(" ", "")))

filtered_despesa_df['IDX_OPFINANCEIRA'] = filtered_despesa_df['IDX_OPFINANCEIRA'].apply(lambda x: int(str(x).replace(" ", "")))
OPFIN['PK_OPFINANCEIRA'] = OPFIN['PK_OPFINANCEIRA'].apply(lambda x: int(str(x).replace(" ", "")))
merged_df = pd.merge(filtered_despesa_df, OPFIN, 
                     left_on='IDX_OPFINANCEIRA', right_on='PK_OPFINANCEIRA', how='left')
grouped_df = merged_df.groupby(['DESCRICAO'])['VALORAPAGAR'].sum().reset_index()
sorted_grouped_df = grouped_df.sort_values(by='VALORAPAGAR', ascending=False).reset_index(drop=True)
sorted_grouped_df['CATEGORIA'] = None

sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'INSS da Empresa', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Cofins', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Icms', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Simples Nacional', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Pis', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Impostos Sobre  Serviços Tomados', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IPTU', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'ISSQN', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IOF', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'ICMS Diferença de Alíquota', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Inss s/retirada pro labore', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Ipva / licenciamento /seguro obrigatório', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'INSS  Retido na fonte', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Inss s/ trab autônomo', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IPI', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IRRF', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IRF', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Impostos, Taxas e Tarifas Públicas e de Conselhos', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IR s/ Ret. Pro-Labore dos Sócios', 'CATEGORIA'] = 'Impostos sobre receita'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IRPF', 'CATEGORIA'] = 'Impostos sobre receita'

#APROPRIAÇÃO MÊS ANTERIOR/ OU DTPREVISAO
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Comissões Sobre vendas Liquidas (Comercial) Vendedoras', 'CATEGORIA'] = 'Comissoes'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Comissões de Vendas p/ Salões e Cerimoniais', 'CATEGORIA'] = 'Comissoes'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Prêmios/Gorjetas', 'CATEGORIA'] = 'Comissoes'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Salários', 'CATEGORIA'] = 'Custo variável - Mão de Obra'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Profissionais e Serviços para Eventos', 'CATEGORIA'] = 'Custo variável - Mão de Obra'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Vale Transporte / Transp. Funcionários ( Parte da Empresa )', 'CATEGORIA'] = 'Custo variável - Mão de Obra'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Férias', 'CATEGORIA'] = 'Custo variável - Mão de Obra'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == '13º Salário', 'CATEGORIA'] = 'Custo variável - Mão de Obra'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Assistência Médica', 'CATEGORIA'] = 'Custo variável - Mão de Obra'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Mão de obra temporária', 'CATEGORIA'] = 'Custo variável - Mão de Obra'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Pagamento de hora extra', 'CATEGORIA'] = 'Custo variável - Mão de Obra'

#FAZER ESTOQUE
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Mercearia', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Carnes', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Bebidas', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Laticínios', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Hotifrutigranjeiros', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Congelados', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Material de embalagem', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Produto para revenda', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Produto semi-acabado', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Produto acabado', 'CATEGORIA'] = 'Custo variável - Materiais e Insumos'

#APROPRIAÇÃO MÊS CORRENTE
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Combustíveis e lubrificantes', 'CATEGORIA'] = 'Custo variável - Logística'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Manutenção e conservação de veículos', 'CATEGORIA'] = 'Custo variável - Logística'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Fretes s/ compra insumo', 'CATEGORIA'] = 'Custo variável - Logística'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Viagens e estadas', 'CATEGORIA'] = 'Custo variável - Logística'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Condução e estacionamento', 'CATEGORIA'] = 'Custo variável - Logística'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Serviços de Transportes Terceirizados/Reembolso KM', 'CATEGORIA'] = 'Custo variável - Logística'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Energia elétrica', 'CATEGORIA'] = 'Custo variável - Operação'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Gás destinado a Eventos/Produção', 'CATEGORIA'] = 'Custo variável - Operação'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Locação Maquinário/Utensílios p/ Eventos', 'CATEGORIA'] = 'Custo variável - Operação'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Água', 'CATEGORIA'] = 'Custo variável - Operação'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Encargos Cartões / Taxas Adm. Cartão', 'CATEGORIA'] = 'Custo variável - Operação'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Adiantamento a Funcionarios', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Contribuição sindical / Conselhos', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Consultoria contábil/jurídica/gerencial e etc', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Tarifas bancarias', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Seguro outros', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Cursos e treinamentos', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Seguro predial', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Seguro de veículos', 'CATEGORIA'] = 'Despesa - Administrativa'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Cópias, reprod. e encad.', 'CATEGORIA'] = 'Despesa - Administrativa'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Manutencao de máq. e equipamentos', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Manutencao de móveis e utensílios', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Manutenção de Prataria', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Material de manutenção', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Conservação e manutenção software e hardware', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Máquinas e equipamentos', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Moveis e Utensilios', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Instalações/Conservação e mant. predial', 'CATEGORIA'] = 'Despesa - Manutenção e TI'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Aquisição de Software e Hardware', 'CATEGORIA'] = 'Despesa - Manutenção e TI'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Despesas com marketing (publicidade, folhetos, etc)', 'CATEGORIA'] = 'Despesa - Comunicação e Marketing'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Assinaturas Diversas Jornais e Revistas', 'CATEGORIA'] = 'Despesa - Comunicação e Marketing'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Correspondências e postagem', 'CATEGORIA'] = 'Despesa - Comunicação e Marketing'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Telefone', 'CATEGORIA'] = 'Despesa - Comunicação e Marketing'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Material de expediente e escritório', 'CATEGORIA'] = 'Despesa - Comunicação e Marketing'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Multas Fiscais', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Despesas processuais (não trabalhistas)', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'TRCT - Rescisão Contrato Trabalho / GFIP Multa RCT', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Processos Trabalhistas / Recursos /  Custas GRU', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Processos Cível (Custos/Recursos/GRU)', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Despesas c/Cartório/Custas Judiciais/Taxas Diversas', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Serviços de terceiros pessoa jurídica', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Multa por Atraso no Pagamento', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Multas Trabalhistas', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Pensão alimentícia', 'CATEGORIA'] = 'Despesa - Legais e Processuais'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Multas de trânsito', 'CATEGORIA'] = 'Despesa - Legais e Processuais'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Cesta Básica', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Material de Higiene e Limpeza', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Material de Uso e Consumo', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Material de Segurança/Uniformes/Primeiros Socorros', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Desp. c/alimentação Funcionários/Diretores e outros', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Material de consumo', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Gás para Máquinas/Refeitório', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Condução / Taxi / Estacionamento', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Fgts', 'CATEGORIA'] = 'Despesa - Funcionários'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Fgts rescisão', 'CATEGORIA'] = 'Despesa - Funcionários'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Devolução de Crédito indevido', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Acerto Financeiro de Eventos', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Devolução de Consignado', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Despesas com aluguel', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Terrenos', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Promoções, Doações, Eventos, Brindes e Cortesias', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Outras Despesas não Especificadas', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Despesas reembolsáveis', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Cheques devolvidos', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Perdas e Prejuízos', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Custo Cobrança', 'CATEGORIA'] = 'Despesa - Financeiras e outros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == '', 'CATEGORIA'] = 'Despesa - Financeiras e outros'

sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Retirada dos sócios', 'CATEGORIA'] = 'Despesa - Sócios'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Empréstimos Sócios', 'CATEGORIA'] = 'Despesa - Sócios'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'DL - Distrib. de Lucro aos Sócios', 'CATEGORIA'] = 'Despesa - Sócios'


#APROPRIAÇÃO TRIMESTRE ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'CSLL', 'CATEGORIA'] = 'Imposto de renda e CSLL'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'IRPJ s/ Faturamento da Empresa', 'CATEGORIA'] = 'Imposto de renda e CSLL'

#APROPRIAÇÃO MÊS ANTERIOR
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Juros de Empréstimos', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Juros/Financiamentos/Atrasos', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Processos Trabalhistas / Recursos / Custas GRU', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Veículos', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Juros s/ descontos de títulos / cheques', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Juros Ref. Aplic. Financ./Remuneração CC', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Fundo Fixo', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Aplicações Financeiras', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Veículos', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Juros de Mútuo para os Sócios', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Consórcios / Títulos de Capitalização', 'CATEGORIA'] = 'Depreciação/Juros'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'PARCELAMENTO DE IMPOSTO', 'CATEGORIA'] = 'Depreciação/Juros'

sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Empréstimos', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Venda de Ativos', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Aportes / emprestimos dos socios', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Receita de Aplicações Financeiras', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Outras Receitas não Operacionais', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Reembolso de despesas', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Resgate de Título de Capitalização', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Parceria', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Resgate Consórcio', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Créditos Indevidos', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Outras Receitas', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Multa Contratual', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Devolução de Empréstimo', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Locação de Materiais', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Liquidação de cobrança', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Receita Financeira de Vendas Juros', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Enc. Funcionarios', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Receita Financeira de Vendas Multas', 'CATEGORIA'] = 'Outras Receitas'
sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Loja  Diamond', 'CATEGORIA'] = 'Outras Receitas'

sorted_grouped_df.loc[sorted_grouped_df['DESCRICAO'] == 'Amortização de empréstimos e financiamentos', 'CATEGORIA'] = 'Amortização'

custom_order = ['Impostos sobre receita', 'Comissoes', 'Custo variável - Mão de Obra','Custo variável - Materiais e Insumos','Custo variável - Logística','Custo variável - Operação','Despesa - Administrativa', 'Despesa - Manutenção e TI','Despesa - Comunicação e Marketing','Despesa - Legais e Processuais','Despesa - Funcionários','Despesa - Financeiras e outros','Despesa - Sócios', 
                'Imposto de renda e CSLL', 'Depreciação/Juros','Outras Receitas','Amortização']

# Create a categorical data type for 'DESCRICAO' based on the custom order
sorted_grouped_df['CATEGORIA'] = pd.Categorical(sorted_grouped_df['CATEGORIA'], categories=custom_order, ordered=True)

# Sort the dataframe based on the custom order
sorted_grouped_df = sorted_grouped_df.sort_values('CATEGORIA').reset_index(drop=True)

sorted_grouped_df['DESCRICAO'] = sorted_grouped_df['DESCRICAO'].str.replace('\x00', '')

merged_df['DESCRICAO'] = merged_df['DESCRICAO'].str.replace('\x00', '')
combined_df = pd.merge(merged_df, sorted_grouped_df[['DESCRICAO', 'CATEGORIA']], on='DESCRICAO', how='left')

# Now, create a new dataframe with only the specified columns
final_df = combined_df[['VALORAPAGAR', 'DTEMISSAO', 'DESCRICAO', 'CATEGORIA']]

# Convert DTEMISSAO to datetime if it isn't already
final_df['DTEMISSAO'] = pd.to_datetime(final_df['DTEMISSAO'])

# Create year-month column for grouping
#final_df['YEAR_MONTH'] = final_df['DTEMISSAO'].dt.to_period('M')
final_df['YEAR_MONTH'] = (final_df['DTEMISSAO'] - pd.DateOffset(months=1)).dt.to_period('M')

# Group by DESCRICAO and YEAR_MONTH and sum the VALORAPAGAR
grouped2 = final_df.groupby(['CATEGORIA', 'YEAR_MONTH'])['VALORAPAGAR'].sum().reset_index()

# Pivot the table to get one column for each month-year with DESCRICAO as rows
pivot_table2 = grouped2.pivot(index='CATEGORIA', columns='YEAR_MONTH', values='VALORAPAGAR').fillna(0)

# Convert the index to a datetime index if it isn't already
RECEITA.index = pd.to_datetime(RECEITA.index)

# Group by Year-Month and sum TOTALDOCTO
monthly_totals = RECEITA.groupby(RECEITA.index.to_period('M'))['TOTALDOCTO'].sum()

# Convert the PeriodIndex to strings with 'YYYY-MM' format
monthly_totals.index = monthly_totals.index.strftime('%Y-%m')
monthly_totals.index = pd.PeriodIndex(monthly_totals.index, freq='M')
# Convert the Series to a DataFrame and transpose it
receita_df = monthly_totals.to_frame().T
receita_df.index = ['Receita']  # This will be the label of the new row

# Ensure the column order matches pivot_table2 before concatenation
# Use the columns from pivot_table2 to reindex receita_df
receita_df = receita_df.reindex(columns=pivot_table2.columns, fill_value=0)

# Concatenate the new DataFrame with pivot_table2, ensuring it appears as the top row
final_table = pd.concat([receita_df, pivot_table2], axis=0)

# Convert the index to a datetime index if it isn't already
RECEITA.index = pd.to_datetime(RECEITA.index)

# Group by Year-Month and sum TOTALDOCTO
monthly_totals = RECEITA.groupby(RECEITA.index.to_period('M'))['TOTALDOCTO'].sum()

# Convert the PeriodIndex to strings with 'YYYY-MM' format
monthly_totals.index = monthly_totals.index.strftime('%Y-%m')
monthly_totals.index = pd.PeriodIndex(monthly_totals.index, freq='M')
# Convert the Series to a DataFrame and transpose it
receita_df = monthly_totals.to_frame().T
receita_df.index = ['Receita']  # This will be the label of the new row

# Ensure the column order matches pivot_table2 before concatenation
# Use the columns from pivot_table2 to reindex receita_df
receita_df = receita_df.reindex(columns=pivot_table2.columns, fill_value=0)

# Concatenate the new DataFrame with pivot_table2, ensuring it appears as the top row
final_table = pd.concat([receita_df, pivot_table2], axis=0)
# Calculate 'Receita Líquida'
final_table.loc['Receita Líquida'] = final_table.loc['Receita'] - final_table.loc['Impostos sobre receita'] - final_table.loc['Comissoes']

# Calculate 'Resultado Bruto Operacional / MC' (MC stands for Margem de Contribuição)
custo_variavel_rows = ['Custo variável - Mão de Obra', 'Custo variável - Materiais e Insumos', 'Custo variável - Logística', 'Custo variável - Operação']
final_table.loc['Resultado Bruto Operacional / MC'] = final_table.loc['Receita Líquida'] - final_table.loc[custo_variavel_rows].sum()

# Calculate 'Resultado Bruto'
despesa_rows = ['Despesa - Administrativa', 'Despesa - Manutenção e TI', 'Despesa - Comunicação e Marketing', 'Despesa - Legais e Processuais', 'Despesa - Funcionários', 'Despesa - Financeiras e outros']
final_table.loc['Resultado Bruto'] = final_table.loc['Resultado Bruto Operacional / MC'] - final_table.loc[despesa_rows].sum()

# Calculate 'Resultado Líquido'
final_table.loc['Resultado Líquido'] = final_table.loc['Resultado Bruto'] - final_table.loc['Imposto de renda e CSLL']

# Calculate 'Resultado Final'
final_table.loc['Resultado Final'] = final_table.loc['Resultado Líquido'] - final_table.loc['Depreciação/Juros'] + final_table.loc['Outras Receitas']

final_table.loc['Fluxo de caixa livre'] = final_table.loc['Resultado Final'] - final_table.loc['Amortização']

new_row_order = [
    'Receita', 'Impostos sobre receita', 'Comissoes', 'Receita Líquida',
    'Custo variável - Mão de Obra', 'Custo variável - Materiais e Insumos', 'Custo variável - Logística', 'Custo variável - Operação', 'Resultado Bruto Operacional / MC',
    'Despesa - Administrativa', 'Despesa - Manutenção e TI', 'Despesa - Comunicação e Marketing', 'Despesa - Legais e Processuais', 'Despesa - Funcionários', 'Despesa - Financeiras e outros', 'Resultado Bruto',
    'Imposto de renda e CSLL', 'Resultado Líquido',
    'Depreciação/Juros','Outras Receitas', 'Resultado Final', 'Amortização', 'Fluxo de caixa livre'
]

final_table = final_table.reindex(new_row_order)

receita_liquida = final_table.iloc[3]
custo_bens_vendidos = final_table.iloc[4] + final_table.iloc[5] + final_table.iloc[6] + final_table.iloc[7]
lucro_liquido = final_table.iloc[17]
despesas = final_table.iloc[9] + final_table.iloc[10] + final_table.iloc[11] + final_table.iloc[12]+ final_table.iloc[13] + final_table.iloc[14]
receita = final_table.iloc[0]
lucro_operacional = final_table.iloc[8]

# Calcular Margem de Lucro Bruto Mensal e Margem Líquida de Lucro Mensal
margem_lucro_bruto_mensal = (receita_liquida - custo_bens_vendidos) / receita_liquida * 100 # na verdade é da resultado bruto / receita
margem_lucro_liquido_mensal = lucro_liquido / receita_liquida * 100
percentual_despesa_da_receita = receita / despesas
retorno_sobre_vendas = lucro_operacional / receita
taxa_crescimento_receita_mensal = receita.pct_change() * 100

# Criar novo DataFrame com as margens
margens_df = pd.DataFrame({
    'Margem de Lucro Bruto Mensal': margem_lucro_bruto_mensal,
    'Margem Líquida de Lucro Mensal': margem_lucro_liquido_mensal,
    'Despesa como percentual da receita': percentual_despesa_da_receita,
    'Margem operacional': retorno_sobre_vendas,
    'Taxa de crescimento mensal': taxa_crescimento_receita_mensal
})
margens_pivot = margens_df.transpose()

def financeiro1():
    return final_table

def financeiro2():
    return margens_pivot