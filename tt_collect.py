from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from pathlib import Path


class MyListener(StreamListener):
    global key_covid
    global key_rio
    global key_sao_paulo

    def filter_tt(self, keyword, text):
        filt_text = None

        words = [x.lower() for x in keyword]
        for word in words:
            if word in text:
                filt_text = text
                break

        return filt_text

    def insert_tt(self, tweet, text, hastags, file, extended=None):
        data_hj = str(datetime.now())[:10]

        obj = {
            "text": text,
        }
        if tweet.get('retweeted_status'):
            obj['retweeted'] = 'Sim'
            obj['retweeted_id'] = tweet["retweeted_status"]["id_str"]
        else:
            obj['retweeted'] = 'Não'
            obj['retweeted_id'] = ''
        if extended:
            obj['created_at'] = tweet["created_at"]
            obj['id_str'] = tweet["id_str"]
            obj['user_id'] = tweet["user"]['id']
            obj['user_name'] = tweet["user"]['name']
            obj['user_location'] = tweet["user"]['location']
            obj['hastags'] = hastags

        json_dados = json.dumps(obj, ensure_ascii=False, indent=3)

        with open(f"dataset/tweets/{data_hj}_{file}.json", '+a', encoding='utf-8') as arquivo:
            if arquivo.tell() == 0:  # Verifica se o Arquivo está vazio.
                arquivo.write(f'[{json_dados}]')
            else:
                pos = arquivo.tell()       # Pega a posição do último caracter.
                pos = pos - 1              # Volta para o caracter "]".
                arquivo.seek(pos)          # Seta o cursor para a posição.
                arquivo.truncate()         # Remove o último caracter.
                arquivo.write(',')         # Adiciona o separador.
                arquivo.write(json_dados)  # Adiciona o objeto JSON.
                arquivo.write(']')         # Fecha a lista.

    def on_data(self, dados):
        tweet = json.loads(dados)

        if tweet.get('retweeted_status'):
            if tweet['retweeted_status'].get('extended_tweet'):
                text = tweet['retweeted_status']['extended_tweet']['full_text'].lower()
            elif tweet['retweeted_status'].get('full_text'):
                text = tweet['retweeted_status']['full_text'].lower()
            else:
                text = tweet['retweeted_status']['text'].lower()
        elif tweet.get('extended_tweet'):
            if tweet['extended_tweet'].get('full_text'):
                text = tweet['extended_tweet']['full_text'].lower()
            else:
                text = tweet['extended_tweet']['text'].lower()
        else:
            if tweet.get('text'):
                text = tweet['text'].lower()
            else:
                text = None

        if text:
            hastags = ''
            if tweet['entities'].get('hashtags'):
                hastags = tweet['entities']['hashtags']

            # Covid Brasil.
            text_covid = self.filter_tt(key_covid, text)
            if text_covid:
                self.insert_tt(tweet, text_covid, hastags, 'covid')

                # Covid Rio de Janeiro.
                text_rio = self.filter_tt(key_rio, text_covid)
                if text_rio:
                    self.insert_tt(tweet, text_rio, hastags, 'covid_rj')

                # Covid São Paulo.
                text_sp = self.filter_tt(key_sao_paulo, text_covid)
                if text_sp:
                    self.insert_tt(tweet, text_sp, hastags, 'covid_sp')

            try:
                if text_covid:
                    print(f"Gravando Covid: User {tweet['user']['name']}")
                tweet.clear()
            except:
                tweet.clear()

        return True


# Palavras Chaves.
key_covid = ['Covid', 'Covid19', 'Covid-19', 'Corona Vírus']
key_rio = ['Rio de Janeiro']
key_sao_paulo = ['São Paulo', 'Sao Paulo']

# Cria uma lista de palavras chaves para buscar nos tweets.
keywords = key_covid + key_rio + key_sao_paulo

# Chaves de autenticação para o Twitter. É necessário o cadastro na API do Twitter.
consumer_key = "SUA CONSUMER KEY DO TWITTER"
consumer_secret = "SUA CONSUMER SECRET DO TWITTER"
access_token = "SEU ACCESS TOKEN DO TWITTER"
access_token_secret = "SEU ACCESS TOKEN SECRET DO TWITTER"

# Prepara a conexão com o Twitter.
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Cria o streamer.
my_listener = MyListener()
my_stream = Stream(auth, listener=my_listener, tweet_mode='extended')

# Coleta os Tweets.
my_stream.filter(track=keywords, languages=["pt"])
