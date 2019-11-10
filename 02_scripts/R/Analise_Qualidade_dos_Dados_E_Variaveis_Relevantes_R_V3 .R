gc(reset=TRUE)

setwd("/home/ds/git/201907/02_scripts/R/") 


# Carregando os pacotes

library(RMySQL)
library(dplyr)
library(data.table)
library(lattice)
library(Matrix)
library(DMwR)
library(readxl)
library(corrplot)
library(stringr)
library(forecast)
library(forecastHybrid)
library(RPostgreSQL)

drv <- dbDriver("PostgreSQL")

pw <- {
  "dsdb2019"
}

conexao <- dbConnect(drv, dbname = "datascience",
                     host = "localhost", 
                     user = "dsdb", password = pw)


# Importando o arquivo proviniente da ETL para Analise da qualidade do dado

export_graos <- data.table(read.csv2("https://raw.githubusercontent.com/datasciencealdeia/201903/master/03_dados/auxiliares/Base_AgroXP_0504.csv", header = T, sep=";"))

View(export_graos)


# Importando o arquivo com os Paises Importadores

paises_import <- data.table(read.table("https://raw.githubusercontent.com/datasciencealdeia/201903/master/03_dados/auxiliares/Paises_Importadores.txt", header = T, sep=";", encoding = "UTF-8"))

View(paises_import)

# Ou atrav?s do Banco de Dados:

ds_pais <- dbGetQuery(conexao, "SELECT * from ds_pais")

View(ds_pais)

# Criando Informacoes das NCM de soja, milho e cafe

ncm <- data.table(CO_NCM=c(12019000,9011110,10059010), Nome=c("Soja","Cafe","Milho"))

View(ncm)



# Analisando a Qualidade dos Dados

## Nomes dos Campos

names(export_graos)



# Valores Maximos e Minimos dos principais campos

## ANO

min(export_graos$CO_ANO)

max(export_graos$CO_ANO)



## Toneladas

min(export_graos$KG_LIQUIDO)

max(export_graos$KG_LIQUIDO)



# Grafico para entender melhor a distribuicao, dado o alto valor de 1798445645 Toneladas

plot(export_graos$KG_LIQUIDO)




## Valor em Dolares

min(export_graos$VL_FOB)

max(export_graos$VL_FOB)




## Cotacao do Dolar

min(export_graos$Fechamento)

max(export_graos$Fechamento)





## Ou prode-se fazer isto no atacado

summary(export_graos)




## Como nao existem problemas aparentes nos dados, eles serao explorados

## Qual e o maior pais Importador geral de Graos em Valor?

import <- data.frame((export_graos %>%
                        group_by(CO_PAIS) %>%
                        dplyr::summarise(Dolares=sum(VL_FOB, NA, na.rm = TRUE)
                        )),
                     row.names = NULL)

import

summary(import)

## Agrupamento gerou varios valores nulos, serao substituidos

import$Dolares[which(is.na(import$Dolares))] <- 0



## Armazenando e encontrando o Pais

m_v <- max(import)

p <- import[import$Dolares == m_v,]

p

## Procurando o pais no arquivo/tabela referencia

m_p_valor <- paises_import[paises_import$CO_PAIS==p[,1],]

m_p_valor




## E o maior importador em Toneladas?

## Qual e o maior pais Importador geral de Graos em Valor?

import_2 <- data.frame((export_graos %>%
                          group_by(CO_PAIS) %>%
                          dplyr::summarise(Toneladas=sum(KG_LIQUIDO, NA, na.rm = TRUE)
                          )),
                       row.names = NULL)

head(import_2,5)

tail(import_2,6)

## Agrupamento gerou varios valores nulos, serao substituidos

import_2$Toneladas[which(is.na(import_2$Toneladas))] <- 0


## Encontrando e armazenando o Pais

m_v <- max(import_2)

t <- import_2[import_2$Toneladas == m_v,]


## Procurando o pais no arquivo/tabela referencia

m_p_ton <- paises_import[paises_import$CO_PAIS==t[,1],]

m_p_ton

## E os maiores em toneladas por tipo de graos

import_3 <- data.frame((export_graos %>%
                          group_by(CO_PAIS,CO_NCM) %>%
                          dplyr::summarise(Toneladas=sum(KG_LIQUIDO, NA, na.rm = TRUE)
                          )),
                       row.names = NULL)

head(import_3,3)

ncm

m_i_soja <- data.table(head(import_3 %>%
                              filter(CO_NCM==12019000) %>%  
                              group_by(CO_PAIS, Grao="Soja") %>%
                              arrange(desc(Toneladas)),1) 
)

m_i_cafe <- data.table(head(import_3 %>%
                              filter(CO_NCM==9011110) %>%  
                              group_by(CO_PAIS, Grao='Cafe') %>%
                              arrange(desc(Toneladas)),1))


m_i_milho <- data.table(head(import_3 %>%
                               filter(CO_NCM==10059010) %>%  
                               group_by(CO_PAIS, Grao="Milho") %>%
                               arrange(desc(Toneladas)),1))
m_i_soja
m_i_cafe
m_i_milho

m_i_graos <- full_join(m_i_soja, m_i_cafe)

m_i_graos <- full_join(m_i_graos, m_i_milho)

m_i_ton_scm <- paises_import[paises_import$CO_PAIS==m_i_graos[1,1]|
                               paises_import$CO_PAIS==m_i_graos[2,1]|
                               paises_import$CO_PAIS==m_i_graos[3,1]]

m_i_ton_scm <- right_join(m_i_ton_scm, m_i_graos, 'CO_PAIS')

m_i_ton_scm

# E os maiores estados exportadores de Graos em Valor?

export <- data.frame((export_graos %>%
                        group_by(SG_UF_NCM,CO_NCM) %>%
                        dplyr::summarise(Dolares=sum(VL_FOB, NA, na.rm = TRUE)
                        )),
                     row.names = NULL)

export

ncm

m_e_soja <- data.table(head(export %>%
                              filter(CO_NCM==12019000) %>%  
                              group_by(SG_UF_NCM,Grao="Soja") %>%
                              arrange(desc(Dolares)),1))

m_e_cafe <- data.table(head(export %>%
                              filter(CO_NCM==9011110) %>%  
                              group_by(SG_UF_NCM,Grao="Cafe") %>%
                              arrange(desc(Dolares)),1))


m_e_milho <- data.table(head(export %>%
                               filter(CO_NCM==10059010) %>%  
                               group_by(SG_UF_NCM,Grao="Milho") %>%
                               arrange(desc(Dolares)),1))
m_e_soja
m_e_cafe
m_e_milho


## Agora vamos verificar quais variaveis sao mais relevantes para continuarmos com o estudo:

# Correlacao entre quantidade de exportacao e variacao do dolar

cor.test(export_graos$QT_ESTAT,export_graos$Fechamento)


## Grafico de correlacao entre estas variaveis

corrplot.mixed(cor(data.frame(Quantidade=export_graos$QT_ESTAT,Dolar=export_graos$Fechamento)), 
               number.cex = 1.5, upper = 'ellipse')


## Como a variavel dolar explica a quantidade exportada?

qt_vs_dolar <- lm(export_graos$QT_ESTAT~export_graos$Fechamento)

print(qt_vs_dolar)

summary(qt_vs_dolar)


## Estao faltando variaveis para explicar esta relacao, vamos em frente

## Relacao entre pais importador e quantidade exportada

cor.test(export_graos$QT_ESTAT,export_graos$CO_PAIS)



## Grafico de correlacao entre estas variaveis

corrplot.mixed(cor(data.frame(Quantidade=export_graos$QT_ESTAT,Pais=export_graos$CO_PAIS)), 
               number.cex = 1.5, upper = 'ellipse')

## Como uma variavel explica a quantidade importada?

qt_vs_paisr <- lm(export_graos$QT_ESTAT~export_graos$CO_PAIS)

print(qt_vs_dolar)

summary(qt_vs_dolar)




## Como a variavel CO_PAIS e nominal, nao e possivel fazer esta analise

## Quantidade exportada e valor do grao no momento da venda


cor.test(export_graos$QT_ESTAT,export_graos$VL_FOB)


corrplot.mixed(cor(data.frame(Quantidade=export_graos$QT_ESTAT,Valor=export_graos$VL_FOB)), 
               number.cex = 1.5, upper = 'ellipse')


qt_vs_valor <- lm(export_graos$QT_ESTAT~export_graos$VL_FOB)

print(qt_vs_dolar)

summary(qt_vs_dolar)





# E se verificarmos Valor e dolar juntos com quantidade?


corrplot.mixed(cor(data.frame(Quantidade=export_graos$QT_ESTAT,Valor=export_graos$VL_FOB, 
                              Dolar=export_graos$Fechamento)), number.cex = 1.5, upper = 'ellipse')


qt_vs_valor_e_dolar <- lm(export_graos$QT_ESTAT~export_graos$VL_FOB+export_graos$Fechamento)

print(qt_vs_valor_e_dolar)

summary(qt_vs_valor_e_dolar)


# Conseguimos cruzar todas as variaveis e validar todas de uma unica vez?





## Basta separar todas as variaveis numericas numa unica tabela


tab_cor <- data.frame(Quantidade=export_graos$QT_ESTAT,
                      Valor=export_graos$VL_FOB, 
                      Dolar=export_graos$Fechamento, 
                      Toneladas=export_graos$KG_LIQUIDO)


## E fazer a analise de correlacao

corrplot.mixed(cor(tab_cor), number.cex = 1.5, upper = 'ellipse')




## Aqui verifica-se que existe relacao direta entre toneladas comercializadas e o valor do dolar

ton_vs_dolar <- lm(export_graos$KG_LIQUIDO~export_graos$VL_FOB)

print(ton_vs_dolar)

summary(ton_vs_dolar)



## Neste caso nao existem variaveis neste conjunto, seria necessario separar por graos e fazer a mesma validacao

# Conclusao: Existem outras variaveis que necessitam ser encontradas e anexadas para explicar as vendas de graos