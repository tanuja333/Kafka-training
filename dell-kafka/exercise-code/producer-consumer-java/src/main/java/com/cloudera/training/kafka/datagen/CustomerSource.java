package com.cloudera.training.kafka.datagen;

import com.cloudera.datagen.impl.CoreRandomImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.math3.random.RandomDataGenerator;

public class CustomerSource {
    
    private final RandomDataGenerator random = CoreRandomImpl.getGenerator();
    
    private int customerId;
    private final List<String> fnames = getFirstNames();
    private final List<String> lnames = getLastNames();
    private final List<String> codes = getAreaCodes();

    
    public CustomerSource() {
        this.customerId = 185000;
    }
    
    public String getNewCustomerInfo() {
        customerId++;
        
        String fname = fnames.get(random.nextInt(0, fnames.size() - 1));
        String lname = lnames.get(random.nextInt(0, lnames.size() - 1));
        String phone = getPhoneNumber();
        
        return customerId + "," + fname + "," + lname + "," + phone;
    }
    
    private String getPhoneNumber() {
        String phone = codes.get(random.nextInt(0, codes.size() - 1))
                + "-555-"
                + random.nextInt(2, 9)
                + random.nextInt(2, 9)
                + random.nextInt(2, 9)
                + random.nextInt(2, 9);

         return phone;
    }
    
    private static List<String> getAreaCodes() {
        List<String> codes = new ArrayList<>();
        
        codes.add("206");
        codes.add("209");
        codes.add("213");
        codes.add("279");
        codes.add("310");
        codes.add("323");
        codes.add("408");
        codes.add("415");
        codes.add("425");
        codes.add("480");
        codes.add("503");
        codes.add("509");
        codes.add("510");
        codes.add("602");
        codes.add("619");
        codes.add("626");
        codes.add("650");
        codes.add("702");
        codes.add("707");
        codes.add("714");
        codes.add("775");
        codes.add("801");
        codes.add("805");
        codes.add("818");
        codes.add("909");
        codes.add("916");
        codes.add("310");
        
        
        return codes;
    }
    
    
    
    
    
    
    
    
public static final String[] FIRSTNAMES = {
        "Aaron", "Adam", "Adrian", "Alan", "Albert", "Alex", "Alexander", "Alfred", 
        "Allan", "Allen", "Alvin", "Andre", "Andrew", "Angel", "Anthony", "Antonio", 
        "Arnold", "Arthur", "Barry", "Ben", "Benjamin", "Bernard", "Bill", "Billy", 
        "Bobby", "Brad", "Bradley", "Brandon", "Brent", "Brett", "Brian", "Bruce", 
        "Bryan", "Calvin", "Carl", "Carlos", "Cecil", "Chad", "Charles", "Charlie", 
        "Chester", "Chris", "Christian", "Christopher", "Clarence", "Claude", "Clifford", 
        "Clinton", "Clyde", "Cody", "Corey", "Cory", "Craig", "Curtis", "Dale", "Dan", 
        "Daniel", "Danny", "Darrell", "Darren", "Darryl", "David", "Dean", "Dennis", 
        "Derek", "Derrick", "Don", "Donald", "Douglas", "Duane", "Dustin", "Earl", 
        "Eddie", "Edgar", "Edward", "Edwin", "Elmer", "Eric", "Erik", "Ernest", 
        "Eugene", "Fernando", "Floyd", "Francis", "Francisco", "Frank", "Franklin", 
        "Fred", "Frederick", "Gabriel", "Gary", "Gene", "George", "Gerald", "Gilbert", 
        "Glen", "Glenn", "Gordon", "Greg", "Gregory", "Harold", "Harry", "Harvey", 
        "Hector", "Henry", "Herbert", "Herman", "Howard", "Jack", "Jacob", "James", 
        "Jamie", "Jared", "Jason", "Javier", "Jay", "Jeff", "Jeffery", "Jeffrey", 
        "Jeremy", "Jerome", "Jerry", "Jesse", "Jessie", "Jesus", "Jim", "Jimmy", 
        "Joe", "Joel", "John", "Johnny", "Jon", "Jonathan", "Jorge", "Jose", "Joseph", 
        "Joshua", "Juan", "Julio", "Justin", "Karl", "Keith", "Kelly", "Kenneth", 
        "Kevin", "Kurt", "Kyle", "Lance", "Larry", "Lawrence", "Lee", "Leo", "Leon", 
        "Leonard", "Leroy", "Leslie", "Lester", "Lewis", "Lloyd", "Lonnie", "Louis", 
        "Luis", "Manuel", "Marc", "Marcus", "Mario", "Mark", "Martin", "Marvin", 
        "Matthew", "Maurice", "Melvin", "Michael", "Micheal", "Miguel", "Mike", 
        "Milton", "Mitchell", "Nathan", "Nathaniel", "Neil", "Nicholas", "Norman", 
        "Oscar", "Patrick", "Paul", "Pedro", "Peter", "Philip", "Phillip", "Rafael", 
        "Ralph", "Ramon", "Randall", "Randy", "Raul", "Ray", "Raymond", "Reginald", 
        "Ricardo", "Richard", "Rick", "Ricky", "Robert", "Roberto", "Rodney", "Roger",
        "Roland", "Ron", "Ronald", "Ronnie", "Roy", "Ruben", "Russell", "Ryan", "Sam", 
        "Samuel", "Scott", "Sean", "Shane", "Shawn", "Stanley", "Stephen", "Steve", 
        "Steven", "Ted", "Terry", "Theodore", "Thomas", "Tim", "Timothy", "Todd", 
        "Tom", "Tommy", "Tony", "Travis", "Troy", "Tyler", "Tyrone", "Vernon", 
        "Victor", "Vincent", "Walter", "Warren", "Wayne", "Wesley", "William", 
        "Willie", "Zachary", 
        "Mary", "Patricia", "Linda", "Barbara", "Elizabeth", "Jennifer", "Maria", 
        "Susan", "Margaret", "Dorothy", "Lisa", "Nancy", "Karen", "Betty", "Helen", 
        "Sandra", "Donna", "Carol", "Ruth", "Sharon", "Michelle", "Laura", "Sarah", 
        "Kimberly", "Deborah", "Jessica", "Shirley", "Cynthia", "Angela", "Melissa", 
        "Brenda", "Amy", "Anna", "Rebecca", "Virginia", "Kathleen", "Pamela", "Martha", 
        "Debra", "Amanda", "Stephanie", "Carolyn", "Christine", "Marie", "Janet", 
        "Catherine", "Frances", "Ann", "Joyce", "Diane", "Alice", "Julie", "Heather", 
        "Teresa", "Doris", "Gloria", "Evelyn", "Jean", "Cheryl", "Mildred", "Katherine", 
        "Joan", "Ashley", "Judith", "Rose", "Janice", "Kelly", "Nicole", "Judy", 
        "Christina", "Kathy", "Theresa", "Beverly", "Denise", "Tammy", "Irene", "Jane", 
        "Lori", "Rachel", "Marilyn", "Andrea", "Kathryn", "Louise", "Sara", "Anne", 
        "Jacqueline", "Wanda", "Bonnie", "Julia", "Ruby", "Lois", "Tina", "Phyllis", 
        "Norma", "Paula", "Diana", "Annie", "Lillian", "Emily", "Robin", "Peggy", 
        "Crystal", "Gladys", "Rita", "Dawn", "Connie", "Florence", "Tracy", "Edna", 
        "Tiffany", "Carmen", "Rosa", "Cindy", "Grace", "Wendy", "Victoria", "Edith", 
        "Kim", "Sherry", "Sylvia", "Josephine", "Thelma", "Shannon", "Sheila", "Ethel", 
        "Ellen", "Elaine", "Marjorie", "Carrie", "Charlotte", "Monica", "Esther", 
        "Pauline", "Emma", "Juanita", "Anita", "Rhonda", "Hazel", "Amber", "Eva", 
        "Debbie", "April", "Leslie", "Clara", "Lucille", "Jamie", "Joanne", "Eleanor", 
        "Valerie", "Danielle", "Megan", "Alicia", "Suzanne", "Michele", "Gail", "Bertha", 
        "Darlene", "Veronica", "Jill", "Erin", "Geraldine", "Lauren", "Cathy", "Joann", 
        "Lorraine", "Lynn", "Sally", "Regina", "Erica", "Beatrice", "Dolores", "Bernice", 
        "Audrey", "Yvonne", "Annette", "June", "Samantha", "Marion", "Dana", "Stacy", 
        "Ana", "Renee", "Ida", "Vivian", "Roberta", "Holly", "Brittany", "Melanie", 
        "Loretta", "Yolanda", "Jeanette", "Laurie", "Katie", "Kristen", "Vanessa", 
        "Alma", "Sue", "Elsie", "Beth", "Jeanne", "Vicki", "Carla", "Tara", "Rosemary", 
        "Eileen", "Terri", "Gertrude", "Lucy", "Tonya", "Ella", "Stacey", "Wilma", 
        "Gina", "Kristin", "Jessie", "Natalie", "Agnes", "Vera", "Willie", "Charlene", 
        "Bessie", "Delores", "Melinda", "Pearl", "Arlene", "Maureen", "Colleen", "Allison", 
        "Tamara", "Joy", "Georgia", "Constance", "Lillie", "Claudia", "Jackie", "Marcia", 
        "Tanya", "Nellie", "Minnie", "Marlene", "Heidi", "Glenda", "Lydia", "Viola", 
        "Courtney", "Marian", "Stella", "Caroline", "Dora", "Jo"
    };
    
    public static final String[] LASTNAMES = {
        "Abbott", "Adams", "Adkins", "Aguilar", "Alexander", "Allen", "Allison", 
        "Alvarado", "Alvarez", "Anderson", "Andrews", "Armstrong", "Arnold", "Atkins", 
        "Austin", "Bailey", "Baker", "Baldwin", "Ball", "Ballard", "Banks", "Barber", 
        "Barker", "Barnes", "Barnett", "Barrett", "Barton", "Bass", "Bates", "Beck", 
        "Becker", "Bell", "Bennett", "Benson", "Berry", "Bishop", "Black", "Blair", 
        "Blake", "Boone", "Bowen", "Bowers", "Bowman", "Boyd", "Bradley", "Brady", 
        "Brewer", "Bridges", "Briggs", "Brock", "Brooks", "Brown", "Bryan", "Bryant", 
        "Buchanan", "Burgess", "Burke", "Burns", "Burton", "Bush", "Butler", "Byrd", 
        "Cain", "Caldwell", "Campbell", "Cannon", "Carlson", "Carpenter", "Carr", 
        "Carroll", "Carson", "Carter", "Casey", "Castillo", "Castro", "Chambers", 
        "Chandler", "Chapman", "Chavez", "Christensen", "Clark", "Clarke", "Clayton", 
        "Cobb", "Cohen", "Cole", "Coleman", "Collier", "Collins", "Conner", "Cook", 
        "Cooper", "Copeland", "Cortez", "Cox", "Craig", "Crawford", "Cross", "Cruz", 
        "Cummings", "Cunningham", "Curry", "Curtis", "Daniel", "Daniels", "Davidson", 
        "Davis", "Dawson", "Day", "Dean", "Delgado", "Dennis", "Diaz", "Dixon", 
        "Douglas", "Doyle", "Drake", "Duncan", "Dunn", "Edwards", "Elliott", "Ellis", 
        "Erickson", "Estrada", "Evans", "Farmer", "Ferguson", "Fernandez", "Fields", 
        "Figueroa", "Fisher", "Fitzgerald", "Fleming", "Fletcher", "Flores", "Flowers", 
        "Floyd", "Ford", "Foster", "Fowler", "Fox", "Francis", "Frank", "Franklin", 
        "Frazier", "Freeman", "French", "Fuller", "Garcia", "Gardner", "Garner", 
        "Garrett", "Garza", "George", "Gibbs", "Gibson", "Gilbert", "Gill", "Glover", 
        "Gomez", "Gonzales", "Gonzalez", "Goodman", "Goodwin", "Gordon", "Graham", 
        "Grant", "Graves", "Gray", "Green", "Greene", "Greer", "Gregory", "Griffin", 
        "Griffith", "Gross", "Guerrero", "Gutierrez", "Guzman", "Hale", "Hall", 
        "Hamilton", "Hammond", "Hampton", "Hansen", "Hanson", "Hardy", "Harmon", 
        "Harper", "Harrington", "Harris", "Harrison", "Hart", "Harvey", "Hawkins", 
        "Hayes", "Haynes", "Henderson", "Henry", "Hernandez", "Herrera", "Hicks", 
        "Higgins", "Hill", "Hines", "Hodges", "Hoffman", "Hogan", "Holland", 
        "Holloway", "Holmes", "Holt", "Hopkins", "Horton", "Houston", "Howard", 
        "Howell", "Hubbard", "Hudson", "Huff", "Hughes", "Hunt", "Hunter", "Ingram", 
        "Jackson", "Jacobs", "James", "Jefferson", "Jenkins", "Jennings", "Jensen", 
        "Jimenez", "Johnson", "Johnston", "Jones", "Jordan", "Joseph", "Keller",
        "Kelley", "Kelly", "Kennedy", "Kim", "King", "Klein", "Knight", "Lamb",
        "Lambert", "Lane", "Larson", "Lawrence", "Lawson", "Lee", "Leonard", 
        "Lewis", "Lindsey", "Little", "Lloyd", "Logan", "Long", "Lopez", "Love", 
        "Lowe", "Lucas", "Luna", "Lynch", "Lyons", "Mack", "Maldonado", "Malone", 
        "Mann", "Manning", "Marsh", "Marshall", "Martin", "Martinez", "Mason", 
        "Massey", "Mathis", "Matthews", "Maxwell", "May", "McBride", "McCarthy", 
        "McCormick", "McCoy", "McDaniel", "McDonald", "McGee", "McGuire", "McKenzie", 
        "McKinney", "Medina", "Mendez", "Mendoza", "Meyer", "Miles", "Miller", 
        "Mills", "Mitchell", "Montgomery", "Moody", "Moore", "Morales", "Moran", 
        "Moreno", "Morgan", "Morris", "Morrison", "Morton", "Moss", "Mullins", 
        "Munoz", "Murphy", "Murray", "Myers", "Nash", "Neal", "Neff", "Nelson", 
        "Newman", "Newton", "Nguyen", "Nichols", "Norman", "Norris", "Norton", 
        "Nunez", "O'Brien", "O'Bryen", "Oliver", "Olson", "Ortega", "Ortiz", 
        "Osborne", "Owen", "Owens", "Padilla", "Page", "Palmer", "Park", "Parker", 
        "Parks", "Parsons", "Patrick", "Patterson", "Patton", "Paul", "Payne", 
        "Pearson", "Pena", "Perez", "Perkins", "Perry", "Peters", "Peterson", 
        "Phelps", "Phillips", "Pierce", "Pittman", "Poole", "Pope", "Porter", 
        "Potter", "Powell", "Powers", "Pratt", "Price", "Quinn", "Ramirez", 
        "Ramos", "Ramsey", "Ray", "Reddy", "Reed", "Reese", "Reeves", "Reid", 
        "Reyes", "Reynolds", "Rhodes", "Rice", "Richards", "Richardson", "Riley", 
        "Rios", "Rivera", "Robbins", "Roberson", "Roberts", "Robertson", "Robinson", 
        "Rodgers", "Rodriguez", "Rodriquez", "Rogers", "Romero", "Rose", "Ross", 
        "Rowe", "Roy", "Ruiz", "Russell", "Ryan", "Salazar", "Sanchez", "Sanders", 
        "Sandoval", "Santiago", "Santos", "Saunders", "Schmidt", "Schneider", 
        "Schultz", "Schwartz", "Scott", "Sharp", "Shaw", "Shelton", "Sherman", 
        "Silva", "Simmons", "Simon", "Simpson", "Sims", "Singleton", "Smith", 
        "Snyder", "Soto", "Sparks", "Spencer", "Stanley", "Steele", "Stephens", 
        "Stevens", "Stevenson", "Stewart", "Stokes", "Stone", "Strickland", 
        "Sullivan", "Summers", "Sutton", "Swanson", "Tate", "Taylor", "Terry", 
        "Thomas", "Thompson", "Thornton", "Todd", "Torres", "Townsend", "Tran", 
        "Tucker", "Turner", "Tyler", "Underwood", "Valdez", "Vargas", "Vasquez", 
        "Vaughn", "Vega", "Wade", "Wagner", "Walker", "Wallace", "Walsh", "Walters", 
        "Walton", "Ward", "Warner", "Warren", "Washington", "Waters", "Watkins", 
        "Watson", "Watts", "Weaver", "Webb", "Weber", "Webster", "Welch", "Wells", 
        "West", "Wheeler", "White", "Wilkerson", "Wilkins", "Williams", "Williamson", 
        "Willis", "Wilson", "Wise", "Wolfe", "Wong", "Wood", "Woods", "Wright", 
        "Yates", "Young", "Zimmerman"
    };
    
    public static List<String> getFirstNames() {
        return Arrays.asList(FIRSTNAMES);
    }
    
    public static List<String> getLastNames() {
        return Arrays.asList(LASTNAMES);
    }
}
