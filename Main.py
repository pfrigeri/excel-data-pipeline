import sqlite3 as db

def connectDB():
    try:
        conn = db.connect("DadosERP.db")
        return conn
    except db.Error as error:
        print(f"Erro ao conectar o banco {error}")
        pass

def readFileCSV(path):
    file = open(path,"r", encoding="utf-8")
    file.readline() #Pula o cabeçalho
    line = file.readline().rstrip()
    dados = []
    while line != "":
        content = line.split(";")
        dados.append(content)
        line = file.readline().rstrip()
    file.close()
    return dados

def clearDataRepress(oldList):
    newList = []
    for line in oldList:
        codRepress = int(line[0])
        tipoPess = line[1]
        nomeFan = line[2]
        # Converter usando Decimal e limitar para 4 casas
        comissaoBase = round(float(line[3].replace(",", ".")), 4)
        newList.append([codRepress, tipoPess, nomeFan, comissaoBase])
    return newList
def to_float(value):
    try:
        if value is None:
            return None
        return float(str(value).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None

def to_int(value):
    try:
        if value is None:
            return None
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None

def clearDataProdutos(oldList):
    newList = []
    for line in oldList:
        try:
            codProd = to_int(line[0])
            nomeProd = line[1].strip() if line[1] else None
            codForne = to_int(line[2])
            unidade = to_int(line[3])
            aliQimics = to_float(line[4])
            valCusto = to_float(line[5])
            valVenda = to_float(line[6])
            qtdeMin = to_int(line[7])
            qtdeEstq = to_int(line[8])
            grupo = to_int(line[9])
            classEstq = line[10].strip() if line[10] else None
            comissao = to_float(line[11])
            pesoBruto = round(to_float(line[12]) or 0.0, 3)

            newList.append([
                codProd, nomeProd, codForne, unidade, aliQimics,
                valCusto, valVenda, qtdeMin, qtdeEstq,
                grupo, classEstq, comissao, pesoBruto
            ])
        except Exception as e:
            print(f"[Erro ao processar linha] {line}")
            print(f" -> Detalhes: {e}")
            continue
    return newList

def clearDataFornClient(oldList):
    newlist = []
    for line in oldList:
        codCliFor = int(line[0].replace(".","")) if line[0] else None
        tipoCF = int(line[1]) if line[1] else None
        codRepres = int(line[2]) if line[2] else None
        nomeFan = line[3]
        cidade = line[4]
        uf = line[5]
        codMunicipio = int(line[6]) if line[6] else None
        tipoPessoa = int(line[7]) if line[7] else None
        cobranc = float(line[8].replace(",", ".")) if line[8] else None
        prazoPgto = int(line[9]) if line[9] else None
        newlist.append([
            codCliFor, tipoCF, codRepres,
            nomeFan, cidade, uf,
            codMunicipio, tipoPessoa, cobranc, prazoPgto
        ])
    return newlist


def dropTable(tableName):
    sql = f"DROP TABLE IF EXISTS {tableName}"
    table = tableName
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print("Tabela deletada com sucesso!!")
    except db.Error as error:
        print(f"Erro ao deletar tabela {error}")
    finally:
        cursor.close()
        conn.close()

#Dados tratados
    #Criar um load geral de tabelas
    
def loadData(tablePath):
    lists = readFileCSV(tablePath)
    
def loadRepressData():
    lists = readFileCSV("TabelasCSV/Repres.csv")
    return clearDataRepress(lists)

def loadProdutosData():
    list = readFileCSV("TabelasCSV/Produtos.csv")
    return clearDataProdutos(list)

def loadFornClient():
    lists = readFileCSV("TabelasCSV/FornClien.csv")
    return clearDataFornClient(lists)

def showData(lists):
    for value in lists:
        print(value)


def dropTable(tableName):
    sql = f"DROP TABLE IF EXISTS {tableName}"
    table = tableName
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print("Tabela deletada com sucesso!!")
    except db.Error as error:
        print(f"Erro ao deletar tabela {error}")
    finally:
        cursor.close()
        conn.close()

def createTableRepress():
    sql = """
            CREATE TABLE IF NOT EXISTS Repress (
                CodRepress INTEGER PRIMARY KEY,
                Tipopess TEXT,
                NomeFan TEXT,
                ComissaoBase REAL
            );
    """
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.execute(sql)
        print("Tabela criada com sucesso!!!")
    except db.Error as error:
        print(f"Erro ao criar tabela repress: {error}")
    finally:
        cursor.close()
        conn.close()

def createTableFornClient():
    sql = """
            CREATE TABLE IF NOT EXISTS FornClient(
                CodCliFor INTEGER PRIMARY KEY AUTOINCREMENT,
                TipoCF TEXT,
                CodRepress INTEGER,
                NomeFan TEXT,
                Cidade TEXT,
                UF TEXT,
                CodMunicipio TEXT,
                TipoPessoa TEXT,
                Cobranc INTEGER,
                PrazoPgto INTEGER,
                FOREIGN KEY (CodRepress) REFERENCES Repress(CodRepress)
            );
        """    
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.execute(sql)
        print("Tabela FornClient criada com sucesso!")
    except db.Error as error:
        print(f"Erro ao criar tabela FornClient: {error}")
    finally:
        cursor.close()
        conn.close()


def createTableProdutos():
    sql = """
        CREATE TABLE IF NOT EXISTS Produtos (
            CodProdutos INTEGER PRIMARY KEY AUTOINCREMENT,
            NomeProduto text,
            CodForne TEXT,
            Unidade INTEGER,
            AliQICMS REAL,
            ValCusto REAL,
            ValVenda REAL,
            qTDEMIN INTEGER,
            QtdeEstq INTEGER,
            Grupo INTEGER,
            ClassEstq TEXT,
            Comissao REAL,
            PesoBruto REAL
        );
    """
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.execute(sql)
    except db.Error as error:
        print(f"Erro ao criar tabela Produtos {error}")
    finally:
        cursor.close()
        conn.close()

def findAllRepress():
    sql = "SELECT * FROM Repress"
    dataRepress = []
    try:
        conn = connectDB()
        cursor = conn.execute(sql)
        dataRepress = cursor.fetchall()
        return dataRepress
    except db.Error as error:
        print(f"Erro ao ler tablea repress")
        return None
    finally:
        cursor.close()
        conn.close()


def insertRepress(listOfRepress):
    sql = """
            INSERT INTO Repress (CodRepress,TipoPess, nomeFan, ComissaoBase)
            VALUES (?, ?, ?, ?)
    """
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.executemany(sql, listOfRepress)
        conn.commit() #Tinha Esqucido o commit rsrs
        print("Dados inseridos na Tabela FornRepress com sucesso!!!")
    except db.Error as error:
        print(f"Erro ao inserir dados na tabela repress: {error}")
    finally:
        cursor.close()
        conn.close()

def insertFornClient(listOfFornClient):
    sql = """
            INSERT INTO FornClient (CodCliFor, TipoCF, CodRepress, NomeFan, Cidade, UF, CodMunicipio, TipoPessoa, Cobranc, PrazoPgto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.executemany(sql, listOfFornClient)
        conn.commit()
        print("Dados inseridos com sucessso!!!")
    except db.Error as error:
        print(f"Erro ao inserir dados na tabela fornClient: {error}")
    finally:
        cursor.close()
        conn.close()

        
def insertProdutos(listOfProducts):
    # Inserção dos dados dos produtos no banco de dados.
    sql = """
        INSERT INTO Produtos (
            CodProdutos, NomeProduto, CodForne, Unidade, AliQICMS,
            ValCusto, ValVenda, Qtdemin, QtdeEstq, Grupo,
            ClassEstq, Comissao, PesoBruto
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        conn = connectDB()
        cursor = conn.cursor()
        cursor.executemany(sql, listOfProducts)
        conn.commit()
        print("Produtos inseridos na tabela Produtos")
    except db.Error as error:
        print(f"Erro ao inserir produtos: {error}")
    finally:
        cursor.close()
        conn.close()
        
def main():
    
    # createTableRepress()
    # insertRepress(loadRepressData())
    # showData(findAllRepress())
    
    # dropTable("FornClient")
    # createTableFornClient()
    # # insertFornClient(loadFornClient())
    dropTable("Produtos")
    createTableProdutos()
    insertProdutos(loadProdutosData())

if __name__ == "__main__":
    main()

