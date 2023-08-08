
import streamlit as st
from pycaret.regression import load_model

# Load the model
model = load_model('/content/drive/MyDrive/IMDB Project/non_review_analysis/models/final_model.pkl')

def main():
    st.title("Movie Profit Prediction App")







    # User input
    st.sidebar.header("Movie Details")
    director = st.sidebar.text_input("Director")
    writers = st.sidebar.text_input("Writers")
    stars = st.sidebar.text_input("Stars")
    production_companies = st.sidebar.text_input("Production Companies")
    genres = st.sidebar.text_input("Genres")
    budget = st.sidebar.number_input("Budget")
    plot_summary_topic = st.sidebar.text_input("Plot Summary Topic")
    countries_of_origin = st.sidebar.text_input("Countries of Origin")
    runtime = st.sidebar.number_input("Runtime")
    metascore = st.sidebar.number_input("Metascore")





    # Prediction
        if st.sidebar.button("Predict"):
            
            user_input = {'director': director, 'writers': writers, 'stars': stars,                 'production_companies': production_companies, 'genres': genres, 'budget':               budget, 'plot_summary_topic': plot_summary_topic, 'countries_of_origin':                countries_of_origin, 'runtime': runtime, 'metascore': metascore}
            
            prediction = model.predict(user_input)






    # Output
    if st.sidebar.button("Predict"):
        # ...
        st.write(f"The predicted profit for this movie is: ${prediction}")



if __name__ == "__main__":
    main()
