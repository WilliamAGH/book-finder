# UML Diagram for Book Recommendation Engine

This directory contains PlantUML files that define the UML diagrams for the Book Recommendation Engine project.

## What is PlantUML?

[PlantUML](https://plantuml.com/) is an open-source tool that allows you to create UML diagrams from a simple text language. It supports various types of UML diagrams, including class diagrams, sequence diagrams, use case diagrams, and more.

## How to Generate UML Diagrams

There are several ways to generate UML diagrams from the PlantUML files:

### Option 1: Online PlantUML Server

1. Go to the [PlantUML Online Server](https://www.plantuml.com/plantuml/uml/)
2. Copy the content of the `.puml` file
3. Paste it into the text area
4. The diagram will be generated automatically

### Option 2: Using IntelliJ IDEA with PlantUML Plugin

1. Install the PlantUML Integration plugin in IntelliJ IDEA
2. Open the `.puml` file
3. Right-click on the editor and select "PlantUML" > "Preview"

### Option 3: Using VS Code with PlantUML Extension

1. Install the PlantUML extension in VS Code
2. Open the `.puml` file
3. Press Alt+D to preview the diagram

### Option 4: Using Command Line

1. Download the PlantUML JAR file from the [PlantUML website](https://plantuml.com/download)
2. Run the following command:
   ```
   java -jar plantuml.jar book_recommendation_engine.puml
   ```
3. This will generate a PNG file with the diagram

## Available UML Diagrams

- `book_recommendation_engine.puml`: Class diagram showing the main classes, their properties, methods, and relationships in the Book Recommendation Engine project.

## Understanding the UML Diagram

The UML class diagram for the Book Recommendation Engine shows the following components:

### Model Classes
- `Book`: Represents a book with properties like title, authors, description, etc.
- `Book.EditionInfo`: Inner class of Book that represents different editions of a book.
- `CachedBook`: JPA entity for caching book data in the database.

### Controller Classes
- `BookController`: REST controller for handling book-related API requests.
- `BookCoverController`: Controller for handling book cover images.
- `BookCoverPreferenceController`: Controller for handling user preferences for book covers.
- `ErrorDiagnosticsController`: Controller for handling error diagnostics.
- `HomeController`: Controller for the home page.
- `ImageResolutionPreferenceController`: Controller for handling image resolution preferences.

### Repository Classes
- `CachedBookRepository`: Interface for accessing CachedBook entities.
- `JpaCachedBookRepository`: JPA implementation of CachedBookRepository.
- `NoOpCachedBookRepository`: No-operation implementation of CachedBookRepository.

### Service Classes
- `GoogleBooksService`: Service for interacting with the Google Books API.
- `BookCacheService`: Service for caching book data.
- `RecentlyViewedService`: Service for tracking recently viewed books.
- `RecommendationService`: Service for generating book recommendations.
- `S3StorageService`: Service for storing data in Amazon S3.
- `BookImageOrchestrationService`: Service for orchestrating book image retrieval.

### Types
- `ImageResolutionPreference`: Enum for image resolution preferences.
- `CoverImageSource`: Enum for cover image sources.

The relationships between these components are shown with arrows in the diagram:
- Solid lines with arrows indicate dependencies (one class uses another).
- Dashed lines with arrows indicate implementations (a class implements an interface).
- Lines with diamonds indicate composition (one class contains another).