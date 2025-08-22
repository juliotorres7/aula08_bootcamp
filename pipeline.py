from etl import pipeline_usuario_final

pasta ='data'
formato_de_saida = ['csv','parquet']

pipeline_usuario_final(pasta,formato_de_saida)
