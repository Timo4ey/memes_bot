import psycopg2

class DataBase_Con:
    def __init__(self, host, user, password, database) -> None:
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
        self.__connection = None
        self.__temp_list_main = None
        self.__temp_list_post_id = None
        self.__for_content_db = None
        self.__for_post_db = None
        self.__temp_list_content_id = None
    

    def connection_on(self) -> list:
        try:
            self.__connection = psycopg2.connect(
                host = self.__host,
                user = self.__user,
                password = self.__password,
                database = self.__database
            )
            self.__connection.autocommit = True
        except Exception as _ex:
            print('[INFO] Got an error', _ex)
        finally:
            if self.__connection:
                print('The connnection in ON')

    def connection_off(self):
        if self.__connection:
            self.__connection.close()
            print('The __connection is closed')
        

    def getIndexesFromMainDB(self) -> list:
        try:
            with self.__connection.cursor() as cursor:
                cursor.execute("""SELECT id FROM main_db;""")
                self.__temp_list_main = cursor.fetchall()
        except Exception as _ex:
            print('[INFO] Got an error', _ex)
        finally:
            print('[INFO]Groups has been gotten. Func getIndexesFromMainDB has finished')
        return self.__temp_list_main



    def GetIDPostsSQL(self) -> list:
        try:
            with self.__connection.cursor() as cursor:
                get_table_query = """SELECT content_id FROM content_db"""
                cursor.execute(get_table_query)
                self.__temp_list_post_id = cursor.fetchall()
                self.__temp_list_post_id = [x[0] for x in self.__temp_list_post_id]
        except Exception as _ex:
            print('[INFO] got an error', _ex)
        finally:
            print('[INFO]content_db has been gotten. Func GetIDPostsSQL has finished')
        return self.__temp_list_post_id

    def GetIDContentSQL(self) -> list:
        try:
            with self.__connection.cursor() as cursor:
                get_table_query = """SELECT content_id FROM content_db"""
                cursor.execute(get_table_query)
                self.__temp_list_content_id = cursor.fetchall()
                self.__temp_list_content_id = [x[0] for x in self.__temp_list_content_id]
        except Exception as _ex:
            print('[INFO] got an error', _ex)
        finally:
            print('[INFO]content_db has been gotten. Func GetIDPostsSQL has finished')
        return self.__temp_list_content_id

    def sendDataContentSQL(self, for_content_db) -> None:
        self.__for_content_db = for_content_db
        try:
            with self.__connection.cursor() as cursor:
                
                for ind in range(len(self.__for_content_db)):
                    insert_postgres = f"""INSERT INTO content_db({', '.join(self.__for_content_db.columns.to_list())})
                    VALUES{tuple(self.__for_content_db.iloc[ind])}"""
                    cursor.execute(insert_postgres)

        except Exception as _ex:
            print('[INFO] got an error', _ex)
        finally:
            print('[INFO] Content_db has been updated. Func sendDataContentSQL has finished')

        
    def sendDataPostsSQL(self, for_post_db) -> None:
        self.__for_post_db = for_post_db
        try:
            with self.__connection.cursor() as cursor:
                
                for ind in range(len(self.__for_post_db)):
                    insert_postgres = f"""INSERT INTO post_db({', '.join(self.__for_post_db.columns.to_list())}) 
                    VALUES{tuple(self.__for_post_db.iloc[ind])}"""
                    cursor.execute(insert_postgres)


        except Exception as _ex:
            print('[INFO] got an error', _ex)
        finally:
            print('[INFO] Post_db has been updated. Func sendDataPostsSQL has finished')
